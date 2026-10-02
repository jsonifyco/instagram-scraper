"""Resumable, page-level collection with fair comment and enrichment queues."""
from __future__ import annotations
from dataclasses import replace
from datetime import datetime
import hashlib
import json
import re
import time
import logging

from .errors import (ChallengeRequiredError, FatalError, InstagramError,
                     LoginRequiredError, NotFoundError, RateLimitedError,
                     UnsupportedUrlError, PageFetchReadTimeoutError)
from .ig import endpoints as ep
from .ig.comment_pages import identity, node
from .ig.network import NetworkCapture
from .ig.observer import ResponseObserver
from .input_model import HAR_FILTER_RESOURCE_TYPES
from .ig import native_comments
from .bro.session import _is_session_fault
from .mappers.comment import map_comment
from .scrapers.base import within_dates
from .mappers.post import map_post
from .shortcode import shortcode_to_media_id
from .urls import parse_target, post_url, resolve_user_id
from .bro import commands as cmd
from .errors import (BroCommandError, BroCommandOutcomeUnknown, BroPayloadError,
                     BroSessionError)
from .budget import BudgetExceeded

log = logging.getLogger(__name__)

POST_FIELDS = {
    "caption": ("caption", "edge_media_to_caption"), "alt": ("accessibility_caption",),
    "likesCount": ("like_count", "edge_media_preview_like", "edge_liked_by"),
    "commentsCount": ("comment_count", "edge_media_to_comment", "edge_media_to_parent_comment"),
    "locationId": ("location", "locations"), "locationName": ("location", "locations"),
    "musicInfo": ("music_metadata", "clips_metadata"),
    "taggedUsers": ("usertags", "edge_media_to_tagged_user"),
    "coauthorProducers": ("coauthor_producers",), "isSponsored": ("is_paid_partnership", "is_ad"),
    "isPinned": ("timeline_pinned_user_ids", "pinned_for_users"), "isCommentsDisabled": ("comments_disabled",),
    "childPosts": ("carousel_media", "edge_sidecar_to_children"), "videoUrl": ("video_versions", "video_url"),
    "videoDuration": ("video_duration",), "videoViewCount": ("view_count", "video_view_count"),
    "videoPlayCount": ("play_count", "video_play_count"), "latestComments": ("preview_comments", "comments", "edge_media_to_parent_comment"),
}
COMMENT_FIELDS = {
    "text": ("text",), "likesCount": ("comment_like_count", "like_count", "edge_liked_by"),
    "repliesCount": ("child_comment_count", "edge_threaded_comments"),
    "media": ("media",), "giphyMediaInfo": ("giphy_media_info",), "isPinned": ("is_pinned",),
}
OWNER_FIELDS = ("id", "username", "full_name", "profile_pic_url", "is_private", "is_verified",
                "fbid_v2", "profile_pic_id", "is_mentionable", "latest_reel_media")


def known_fields(raw, fields):
    return {field for field, names in fields.items() if any(name in raw for name in names)}


def page_cursors(body, replies):
    """REST head and tail retain their independent flags and opaque cursors."""
    directions = (("max_id", "next_max_child_cursor", "has_more_tail_child_comments"),
                  ("min_id", "next_min_child_cursor", "has_more_head_child_comments")) if replies else (
                  ("max_id", "next_max_id", "has_more_comments"),
                  ("min_id", "next_min_id", "has_more_headload_comments"))
    cursors, issues = [], []
    for param, key, flag in directions:
        more, cursor = body.get(flag), body.get(key)
        if not replies and param == "max_id" and not cursor and body.get("next_min_id") and "has_more_headload_comments" not in body:
            continue
        if not replies and param == "min_id" and cursor and "has_more_headload_comments" not in body:
            more = body.get("has_more_comments")
        if more is False:
            continue
        if cursor:
            cursors.append({param: str(cursor)})
        elif more is True:
            issues.append("missing_cursor")
    return cursors, issues


def _har_size(payload):
    """(entries, response text bytes) of one HAR payload -- the data-volume
    counterpart of the observer's ``observerEvents``/``observerBytes``."""
    if not isinstance(payload, dict):
        return 0, 0
    har = payload.get("har_logs", payload)
    entries = ((har.get("log") or {}).get("entries") or []) if isinstance(har, dict) else []
    if not isinstance(entries, list):
        return 0, 0
    text_bytes = 0
    for entry in entries:
        if isinstance(entry, dict):
            content = (entry.get("response") or {}).get("content") or {}
            text_bytes += len(str(content.get("text") or "")) if isinstance(content, dict) else 0
    return len(entries), text_bytes


class CompleteCollector:
    #: Clicks allowed on one thread. A measured 150-reply thread needed ~15;
    #: this is a runaway guard, not an expected limit.
    THREAD_DRAIN_CLICKS = 40
    #: Seconds for a clicked thread to render its next control. Measured at
    #: ~4 s for a single thread loading on its own.
    THREAD_SETTLE_SECONDS = 5.0
    #: Managed reads between telemetry HAR dumps when HAR is the source.
    #: With no ``since`` cutoff the configured resource types remain
    #: cumulative, so polling every few reads would repeat paid export work.
    TRAFFIC_POLL_EVERY = 25
    #: Observation-only checks (5 s each) after a scroll that neither moved
    #: nor delivered a page, before the step is judged. The parent scroll now
    #: lands at the bottom of the list every time, so the DOM cannot "move"
    #: until Instagram appends a page -- and the ceiling probe measured pages
    #: arriving in bursts with gaps of two to three 6-second rounds. Two
    #: checks (16 s with the scroll wait) sit inside such a gap; four cover it.
    UI_SETTLE_CHECKS = 4

    def __init__(self, scrape, storage):
        self.scrape, self.ctx, self.api = scrape, scrape.ctx, scrape.api
        self.config, self.stats = scrape.config, scrape.stats
        self.storage, self.state = storage, storage.state
        self.api.page_store = self.state
        self.ctx.operation_flush = self.storage.dataset.flush
        self.limited_scopes = set()
        self._reused_post_codes = set()
        self.last_traffic_poll = 0
        self.consecutive_command_failures = 0
        self.native_captures = {}
        self.active_ui_scope = None
        #: the in-page response observer, or None when HAR is the source
        self.observer = (ResponseObserver()
                         if getattr(self.config, "response_source", "observer") == "observer"
                         and hasattr(self.ctx, "session") else None)
        #: resource types every HAR export of this run is narrowed to, or
        #: None for the whole archive (``har``). The observer's own dumps
        #: (initial load, verification) are filtered as well: on a six-hour
        #: VM a full export grows with the archive. Only the export is
        #: filtered; ``since`` is never advanced automatically (a request
        #: that started before an export may finish after it).
        self._har_resource_types = (
            None if getattr(self.config, "response_source", "observer") == "har"
            else list(HAR_FILTER_RESOURCE_TYPES))
        #: ``har_filtered`` only: the in-page observer installed as a passive
        #: witness. It feeds nothing into the pages; it is read after every
        #: dump so the filtered export can be compared with what the page
        #: actually received (ids, cursors, terminal responses).
        self._witness = (ResponseObserver()
                         if getattr(self.config, "response_source", "observer") == "har_filtered"
                         and hasattr(self.ctx, "session") else None)
        self._witness_captures = {}
        #: comment ids the filtered export carried for our own managed reads
        #: (getbro's fetch_json runs outside the page's fetch, so the witness
        #: cannot see them by construction; the comparison must not either)
        self._managed_ids = set()
        self._observer_saw = {"parents": False, "replies": False}
        self._observer_installed_at = None
        self._initial_snapshot_ids = set()
        self._last_har_payload = None
        self._observer_pending_operations = []

    def _budget(self):
        if self.ctx.budget:
            self.ctx.budget.check()

    def _operation_event(self, operation):
        def event(status, fields):
            self.state.update_operation(
                operation, status,
                session_id=getattr(self.ctx.session, "session_id", None),
                command_id=fields.get("command_id"), phase=fields.get("phase"),
                last_status=fields.get("last_status"))
            if status in ("recovering", "observation_lost", "outcome_unknown"):
                self.storage.dataset.flush()
        return event

    def _run_ui_command(self, job_id, kind, fingerprint, call):
        """Journal exactly one submitted getbro UI command batch."""
        operation = self.state.begin_operation(
            job_id, kind, hashlib.sha256(fingerprint.encode()).hexdigest(), 1)
        started = time.monotonic()
        try:
            result = call(self._operation_event(operation))
            self.state.update_operation(operation, "applied", phase="ui_result",
                                        last_status="done")
            return result, operation
        except (BroCommandError, BroCommandOutcomeUnknown, BroPayloadError,
                BroSessionError) as exc:
            if kind == "reply_click" and isinstance(exc, BroCommandError):
                failed_job = self.state.get_job(job_id)
                if failed_job:
                    parent = failed_job["payload"].get("parentId")
                    thread = self.state.thread_state(failed_job["scope"], parent) or {}
                    thread.update(phase="visible", last="click_failed")
                    self.state.save_thread_state(failed_job["scope"], parent, thread)
            self.state.update_operation(
                operation,
                "outcome_unknown" if isinstance(exc, BroCommandOutcomeUnknown) else "failed",
                phase=getattr(exc, "phase", None), last_status=getattr(exc, "last_status", None),
                error_type=type(exc).__name__)
            raise
        finally:
            elapsed = time.monotonic() - started
            self.state.add_invocation_metric("uiCommandSeconds", elapsed)
            if kind == "reply_click":
                self.state.add_invocation_metric("replyWaitSeconds", elapsed)
            elif kind == "thread_list":
                self.state.add_invocation_metric("threadListSeconds", elapsed)

    # ------------------------------------------------------------ sources --

    #: decisions of a UI step that end the parent list
    UI_STOPS = ("ui_region_unavailable", "ui_scroll_stalled",
                "ui_pagination_unresponsive", "observer_unconfirmed", "scan_gaps")
    #: extra observer reads (2 s apart) after a click before a thread's own
    #: response is declared missing for this click
    REPLY_RESPONSE_LOOKS = 2

    def _observer_active(self):
        return self.observer is not None and self.observer.available

    def _install_observer(self, job_id):
        """Install or re-confirm the in-page observer; journaled like any
        other browser command. Returns whether it can be used."""
        if self.observer is None:
            return False
        operation = self.state.begin_operation(
            job_id, "observer_install",
            hashlib.sha256(f"observer_install:{job_id}".encode()).hexdigest(), 1)
        try:
            result = self.observer.install(self.ctx.session,
                                           on_event=self._operation_event(operation))
            self.state.update_operation(operation, "applied", phase="install",
                                        last_status="done")
        except (BroCommandError, BroCommandOutcomeUnknown, BroPayloadError,
                BroSessionError) as exc:
            self.state.update_operation(
                operation, "failed", phase=getattr(exc, "phase", None),
                last_status=getattr(exc, "last_status", None), error_type=type(exc).__name__)
            if isinstance(exc, (BroSessionError, BroCommandOutcomeUnknown)) or (
                    isinstance(exc, BroCommandError) and _is_session_fault(exc)):
                raise
            self.observer.available = False
            self.observer.unavailable_reason = type(exc).__name__
            result = {"ok": False}
        if result.get("installed"):
            self.state.add_invocation_metric("observerInstalls", 1)
            self._observer_installed_at = time.time()
            self._observer_saw = {"parents": False, "replies": False}
            self.state.set_meta("observerVerification", None)
        if not self.observer.available:
            self.state.add_invocation_metric("observerUnavailable", 1)
            self._fall_back_to_har("observer_install_failed:"
                                   + str(self.observer.unavailable_reason or "unknown"))
            return False
        elif getattr(self.ctx, "traffic", None) is not None:
            # No periodic HAR dumps while the observer is the source: the
            # request accounting below is partial by construction.
            self.ctx.traffic.partial = True
        return self.observer.available

    def _poll_capture(self, capture, job_id, phase):
        """Bring the capture up to date with what the browser received."""
        if self._observer_active():
            return self._observer_read(capture, job_id, phase)
        if self.observer is not None:
            self._fall_back_to_har("observer_unavailable:"
                                   + str(self.observer.unavailable_reason or "unknown"))
        operation = self._har_poll(capture, job_id, phase)
        self._witness_read(capture, job_id, phase)
        return operation

    def _fall_back_to_har(self, reason):
        """Retire the observer and carry on with filtered cumulative HAR
        dumps as the page source. Nothing is lost by the switch: the archive
        holds every response since the session started, and the fingerprint
        store rejects the pages already committed. The run does not stop."""
        if self.observer is None:
            return
        summary = self.observer.summary()
        self.observer = None
        self._har_resource_types = list(HAR_FILTER_RESOURCE_TYPES)
        record = {"from": "observer", "to": "har_filtered", "reason": str(reason)[:200],
                  "at": time.time(), "observer": summary}
        self.state.set_meta("responseSourceFallback", record)
        self.state.set_meta("observerUnavailable", str(reason)[:200])
        self.state.add_invocation_metric("responseSourceFallbacks", 1)
        traffic = getattr(self.ctx, "traffic", None)
        if traffic is not None:
            # Accounting dumps run again from here, filtered.
            traffic.partial = False
            traffic.resource_types = list(HAR_FILTER_RESOURCE_TYPES)
        log.warning("response observer retired (%s); continuing with filtered HAR dumps", reason)

    def _har_poll(self, capture, job_id, phase):
        """One HAR dump without a time cutoff, for extraction and accounting --
        the whole archive (``har``) or only the configured resource types
        (``har_filtered``); the same dump serves both purposes.

        With the observer as the source this remains only for the initial
        page load, a document change and the one-time verification."""
        operation = self.state.begin_operation(
            job_id, "har", hashlib.sha256(f"har:{job_id}:{phase}".encode()).hexdigest(), 1)
        started = time.monotonic()
        entries = 0
        text_bytes = 0
        try:
            payload = capture.poll(self.ctx.session,
                                   on_event=self._operation_event(operation),
                                   resource_types=self._har_resource_types)
            if capture.disabled:
                self.state.update_operation(operation, "failed", phase="har",
                                            error_type="HarUnavailable")
            else:
                self.state.update_operation(operation, "decoded", phase="har",
                                            last_status="done")
            traffic = getattr(self.ctx, "traffic", None)
            if traffic and payload:
                traffic.observe(payload)
                self.last_traffic_poll = getattr(self.ctx, "api_calls", 0)
            self._last_har_payload = payload
            entries, text_bytes = _har_size(payload)
            return operation
        except (BroCommandError, BroCommandOutcomeUnknown, BroPayloadError,
                BroSessionError) as exc:
            self.state.update_operation(
                operation,
                "outcome_unknown" if isinstance(exc, BroCommandOutcomeUnknown) else "failed",
                phase=getattr(exc, "phase", None), last_status=getattr(exc, "last_status", None),
                error_type=type(exc).__name__)
            raise
        finally:
            seconds = time.monotonic() - started
            self.state.add_invocation_metric("harSeconds", seconds)
            self.state.add_invocation_metric("harDumps", 1)
            self.state.add_invocation_metric(
                "filteredHarDumps" if self._har_resource_types else "fullHarDumps", 1)
            self.state.add_invocation_metric("harEntries", entries)
            self.state.add_invocation_metric("harBytes", text_bytes)
            # Per-dump timings: the archive grows with the session, and
            # whether the filtered export keeps that growth in check is
            # exactly what the comparison with the observer has to show.
            dump_log = list(self.state.get_meta("harDumpLog", []) or [])
            dump_log.append({"phase": str(phase), "seconds": round(seconds, 3),
                             "entries": entries, "bytes": text_bytes,
                             "resourceTypes": self._har_resource_types, "at": time.time()})
            self.state.set_meta("harDumpLog", dump_log[-400:])

    # ---- har_filtered: the observer as a passive witness ------------------

    def _install_witness(self, job_id):
        """Install the in-page observer for comparison only. Best effort: a
        witness that cannot be installed leaves the HAR source untouched."""
        if self._witness is None:
            return False
        operation = self.state.begin_operation(
            job_id, "witness_install",
            hashlib.sha256(f"witness_install:{job_id}".encode()).hexdigest(), 1)
        try:
            result = self._witness.install(self.ctx.session,
                                           on_event=self._operation_event(operation))
            self.state.update_operation(operation, "applied", phase="install",
                                        last_status="done")
        except (BroCommandError, BroCommandOutcomeUnknown, BroPayloadError,
                BroSessionError) as exc:
            self.state.update_operation(
                operation, "failed", phase=getattr(exc, "phase", None),
                last_status=getattr(exc, "last_status", None), error_type=type(exc).__name__)
            if isinstance(exc, (BroSessionError, BroCommandOutcomeUnknown)) or (
                    isinstance(exc, BroCommandError) and _is_session_fault(exc)):
                raise
            self._witness.available = False
            self._witness.unavailable_reason = type(exc).__name__
            result = {"ok": False}
        if result.get("installed"):
            self.state.add_invocation_metric("witnessInstalls", 1)
        if not self._witness.available:
            self.state.set_meta("harWitnessUnavailable", self._witness.unavailable_reason)
        return self._witness.available

    def _witness_read(self, capture, job_id, phase):
        """After a filtered dump, read what the page's own fetch/XHR saw
        into a scratch capture and record the comparison. Acknowledged at
        once: nothing here is a page source, so nothing is lost by it."""
        if self._witness is None or not self._witness.available:
            return
        scope = capture.media_id
        witness = self._witness_captures.get(scope)
        if witness is None:
            witness = NetworkCapture(media_id=scope, newest_first=self.config.is_newest_comments)
            self._witness_captures[scope] = witness
        operation = self.state.begin_operation(
            job_id, "witness_read",
            hashlib.sha256(f"witness:{job_id}:{phase}".encode()).hexdigest(), 1)
        try:
            entries, status = self._witness.read(self.ctx.session,
                                                 on_event=self._operation_event(operation))
            if entries:
                witness.ingest_entries(entries)
            self._witness.delivered_ids |= set(witness.seen_comment_ids)
            if status.get("documentChanged"):
                self.state.add_invocation_metric("witnessDocumentChanges", 1)
            self._witness.ack(self.ctx.session, on_event=self._operation_event(operation))
            self.state.update_operation(operation, "applied", phase="witness", last_status="done")
        except (BroCommandError, BroCommandOutcomeUnknown, BroPayloadError,
                BroSessionError) as exc:
            self.state.update_operation(
                operation, "failed", phase=getattr(exc, "phase", None),
                last_status=getattr(exc, "last_status", None), error_type=type(exc).__name__)
            if isinstance(exc, (BroSessionError, BroCommandOutcomeUnknown)) or (
                    isinstance(exc, BroCommandError) and _is_session_fault(exc)):
                raise
            self._witness.available = False
            self._witness.unavailable_reason = type(exc).__name__
            self.state.set_meta("harWitnessUnavailable", self._witness.unavailable_reason)
            return
        self._note_managed_ids(scope)
        comparisons = dict(self.state.get_meta("harObserverComparison", {}) or {})
        comparisons[str(scope)] = self._compare_sources(capture, witness)
        self.state.set_meta("harObserverComparison", comparisons)

    def _note_managed_ids(self, scope):
        """Comment ids that the last export holds only because of our own
        managed reads -- the same exclusion the observer verification makes."""
        traffic = getattr(self.ctx, "traffic", None)
        payload = self._last_har_payload
        if traffic is None or not hasattr(traffic, "is_managed") or not isinstance(payload, dict):
            return
        har = payload.get("har_logs", payload)
        entries = ((har.get("log") or {}).get("entries") or []) if isinstance(har, dict) else []
        managed = []
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            request = entry.get("request") or {}
            text = str(((request.get("postData") or {}).get("text")) or "")
            if traffic.is_managed(str(request.get("url") or ""), str(request.get("method") or "GET"),
                                  text or None):
                managed.append(entry)
        if not managed:
            return
        scratch = NetworkCapture(media_id=scope, newest_first=self.config.is_newest_comments)
        scratch.ingest_entries(managed)
        self._managed_ids |= set(scratch.seen_comment_ids)

    def _compare_sources(self, har, witness):
        """Ids, cursors and terminal flags decoded from the filtered HAR
        against the same from the witness observer for one post."""
        har_ids = set(har.seen_comment_ids)
        observer_ids = set(witness.seen_comment_ids)
        # The observer only sees the page's own requests made after it was
        # installed: the initial snapshot and our managed reads are HAR's
        # alone by construction.
        only_har = har_ids - observer_ids - self._initial_snapshot_ids - self._managed_ids
        only_observer = observer_ids - har_ids

        def pages(capture):
            result = {}
            for key, (template, _) in capture.native_latest.items():
                result[str(key)] = {"versions": capture.native_versions.get(key, 0),
                                    "cursor": template.get("observedCursor"),
                                    "endCursor": template.get("observedEndCursor"),
                                    "hasNext": template.get("observedHasNext")}
            return result
        har_pages, observer_pages = pages(har), pages(witness)
        keys = set(har_pages) | set(observer_pages)
        disagreeing = sorted(k for k in keys if (
            (har_pages.get(k) or {}).get("endCursor") != (observer_pages.get(k) or {}).get("endCursor")
            or (har_pages.get(k) or {}).get("hasNext") != (observer_pages.get(k) or {}).get("hasNext")))
        return {"harIds": len(har_ids), "observerIds": len(observer_ids),
                "initialIds": len(self._initial_snapshot_ids),
                "managedIds": len(self._managed_ids & har_ids),
                "onlyHar": len(only_har), "onlyHarSample": sorted(only_har)[:10],
                "onlyObserver": len(only_observer), "onlyObserverSample": sorted(only_observer)[:10],
                "harTerminal": sum(1 for v in har_pages.values() if v["hasNext"] is False),
                "observerTerminal": sum(1 for v in observer_pages.values() if v["hasNext"] is False),
                "pageKeys": len(keys), "disagreeingPages": len(disagreeing),
                "disagreeingSample": disagreeing[:10],
                "harResponses": har.responses_seen, "observerResponses": witness.responses_seen,
                "witness": self._witness.summary() if self._witness else None,
                "checkedAt": time.time()}

    def _observer_read(self, capture, job_id, phase):
        """Read the page's queue into the capture. Nothing is acknowledged
        here: :meth:`_checkpoint_capture` does that after the pages are
        saved, so a crash in between re-reads instead of losing them."""
        operation = self.state.begin_operation(
            job_id, "observer_read",
            hashlib.sha256(f"observer:{job_id}:{phase}".encode()).hexdigest(), 1)
        started = time.monotonic()
        try:
            before_ids = set(capture.seen_comment_ids)
            versions = dict(capture.native_versions)
            read_operations = []
            def event_factory():
                self._budget()
                part_operation = operation if not read_operations else self.state.begin_operation(
                    job_id, "observer_read", hashlib.sha256(f"observer:{job_id}:{phase}:{len(read_operations)}".encode()).hexdigest(), 1)
                read_operations.append(part_operation)
                return self._operation_event(part_operation)
            entries, status = self.observer.read(self.ctx.session,
                                                 event_factory=event_factory)
            self._observer_pending_operations.extend(read_operations)
            if status.get("documentChanged") or not self.observer.available:
                # The page changed underneath (or the wrapper was replaced):
                # what the old document received is not in this queue. The
                # cumulative archive has it, so the HAR source takes over.
                if status.get("documentChanged"):
                    self.state.add_invocation_metric("observerDocumentChanges", 1)
                self.state.update_operation(operation, "failed", phase="observer",
                                            error_type="ObserverUnavailable")
                self._fall_back_to_har("document_changed" if status.get("documentChanged")
                                       else "observer_read:" + str(self.observer.unavailable_reason or "unknown"))
                return self._har_poll(capture, job_id, phase)
            if entries:
                capture.ingest_entries(entries)
            self.observer.delivered_ids |= (capture.seen_comment_ids - before_ids)
            if capture.native_versions.get("parents", 0) > versions.get("parents", 0):
                self._observer_saw["parents"] = True
            if any(capture.native_versions.get(k, 0) > versions.get(k, 0)
                   for k in capture.native_versions if k != "parents"):
                self._observer_saw["replies"] = True
            dropped = int(status.get("dropped") or 0)
            if dropped and dropped != self.state.get_meta("observerDropped", 0):
                self.state.set_meta("observerDropped", dropped)
                self.state.set_meta("observerDroppedSeqs", list(status.get("droppedSeqs") or [])[:64])
                self.state.add_invocation_metric("observerDroppedEvents", dropped)
            self.state.add_invocation_metric("observerReads", 1)
            self.state.add_invocation_metric("observerEvents", len(entries))
            self.state.add_invocation_metric("observerBytes", sum(
                len(e["response"]["content"].get("text") or "") for e in entries))
            self.state.update_operation(operation, "decoded", phase="observer",
                                        last_status="done")
            return operation
        except (BroCommandError, BroCommandOutcomeUnknown, BroPayloadError,
                BroSessionError) as exc:
            self.state.update_operation(
                operation,
                "outcome_unknown" if isinstance(exc, BroCommandOutcomeUnknown) else "failed",
                phase=getattr(exc, "phase", None), last_status=getattr(exc, "last_status", None),
                error_type=type(exc).__name__)
            raise
        finally:
            self.state.add_invocation_metric("observerReadSeconds", time.monotonic() - started)

    def _observer_ack(self, job_id):
        """Acknowledge the last read -- only after its pages are durable."""
        if not self._observer_active() or self.observer.pending_ack is None:
            return
        operation = self.state.begin_operation(
            job_id, "observer_ack",
            hashlib.sha256(f"observer_ack:{job_id}:{self.observer.pending_ack}".encode()).hexdigest(), 1)
        try:
            self.observer.ack(self.ctx.session, on_event=self._operation_event(operation))
            if not self.observer.available:
                # The pages this read carried are durable already; the
                # archive covers whatever the lost queue held.
                self.state.update_operation(operation, "failed", phase="ack",
                                            error_type="ObserverUnavailable")
                self._fall_back_to_har("observer_ack:" + str(self.observer.unavailable_reason or "unknown"))
                return
            self.state.update_operation(operation, "applied", phase="ack", last_status="done")
            self.state.add_invocation_metric("observerAcks", 1)
        except (BroCommandError, BroCommandOutcomeUnknown, BroPayloadError,
                BroSessionError) as exc:
            self.state.update_operation(
                operation, "failed", phase=getattr(exc, "phase", None),
                last_status=getattr(exc, "last_status", None), error_type=type(exc).__name__)
            if isinstance(exc, (BroSessionError, BroCommandOutcomeUnknown)) or (
                    isinstance(exc, BroCommandError) and _is_session_fault(exc)):
                raise
            # A failed acknowledgement is harmless: the next read returns the
            # same events and the fingerprint store rejects them as known.

    def _maybe_verify_observer(self, capture, job):
        """Once the observer has delivered a parent page and a reply page,
        compare the comment ids it delivered with one full HAR dump taken
        now. Ids the archive holds for comment operations started after the
        observer was installed, but which never came through it, mean the
        UI uses a path the observer cannot see -- stop with that reason."""
        if (self.observer is None or not self._observer_active()
                or self.state.get_meta("observerVerification")
                or not (self._observer_saw["parents"] and self._observer_saw["replies"])):
            return
        scope = job["scope"]
        scratch = NetworkCapture(media_id=scope, newest_first=self.config.is_newest_comments,
                                 stop_on_rejection=True,
                                 rejection_since=getattr(self.ctx, "authenticated_since", None))
        verification = self._har_poll(scratch, job["id"], "observer_verification")
        # A comparison, not a page source: nothing of it is applied later.
        self.state.update_operation(verification, "applied", phase="observer_verification",
                                    last_status="done")
        if scratch.disabled or not isinstance(self._last_har_payload, dict):
            raise FatalError("observer_unconfirmed: verification HAR unavailable")
        payload = self._last_har_payload or {}
        har = payload.get("har_logs", payload) if isinstance(payload, dict) else {}
        entries = ((har.get("log") or {}).get("entries") or []) if isinstance(har, dict) else []
        since = self._observer_installed_at or 0
        wanted = []
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            request = entry.get("request") or {}
            try:
                started = datetime.fromisoformat(
                    str(entry.get("startedDateTime", "")).replace("Z", "+00:00")).timestamp()
            except (ValueError, TypeError):
                continue
            if started < since:
                continue
            url = str(request.get("url") or "")
            text = str(((request.get("postData") or {}).get("text")) or "")
            traffic = getattr(self.ctx, "traffic", None)
            if traffic is not None and hasattr(traffic, "is_managed") and traffic.is_managed(
                    url, str(request.get("method") or "GET"), text or None):
                # Our own managed read (fetch_json runs outside the page's
                # fetch): it is in the archive by our doing, not the UI's.
                continue
            path = url.split("?", 1)[0]
            rest = bool(re.search(r"/api/v1/media/\d+/comments/(?:\d+/child_comments/)?$", path))
            graphql = path.rstrip("/").endswith(("/graphql/query", "/api/graphql")) and "comment" in text.lower()
            if rest or graphql:
                wanted.append(entry)
        scratch = NetworkCapture(media_id=scope, newest_first=self.config.is_newest_comments)
        scratch.ingest_entries(wanted)
        # A response may complete while the verification dump is being made.
        # Drain the passive queue before deciding it was missed.
        versions = dict(capture.native_versions)
        operation = self._observer_read(capture, job["id"], "verification_tail")
        self._checkpoint_capture(capture, job, operation, versions)
        har_ids = set(scratch.seen_comment_ids)
        missing = har_ids - self.observer.delivered_ids - self._initial_snapshot_ids
        result = {"confirmed": bool(wanted and har_ids and not missing), "harEntries": len(wanted),
                  "harIds": len(har_ids), "observerIds": len(self.observer.delivered_ids),
                  "initialIds": len(self._initial_snapshot_ids), "missing": len(missing),
                  "missingSample": sorted(missing)[:10], "checkedAt": time.time()}
        self.state.set_meta("observerVerification", result)
        if not result["confirmed"]:
            # Preserve the evidence, then let the filtered archive -- which
            # provably carries those ids -- be the source from here on.
            versions = dict(capture.native_versions)
            capture.ingest_entries(wanted)
            self._checkpoint_capture(capture, job, operation, versions)
            self._fall_back_to_har(
                f"observer_unconfirmed: {len(missing)} comment ids present in the "
                "browser archive were never delivered by the in-page observer")

    # -------------------------------------------------------------- pages --

    def _checkpoint_capture(self, capture, parent_job, operation, versions):
        """Durably save every scoped row before another UI command, then
        apply it, then acknowledge the observer read that carried it."""
        scope = parent_job["scope"]
        committed = getattr(capture, "_checkpoint_versions", {})
        versions = {k: max(versions.get(k, 0), committed.get(k, 0))
                    for k in set(versions) | set(committed)}
        pages = []
        with self.state.transaction():
            parent_fresh = capture.native_versions.get("parents", 0) > versions.get("parents", 0)
            parent_latest = capture.native_latest.get("parents")
            if capture.comments or parent_fresh:
                body = {"comments": list(capture.comments.values()), "_parentObservation": {
                    "responsePages": max(0, capture.native_versions.get("parents", 0) - versions.get("parents", 0)),
                    "hasNextPage": parent_latest[0].get("observedHasNext") if parent_latest else None,
                    "endCursor": parent_latest[0].get("observedEndCursor") if parent_latest else None}}
                page_id = hashlib.sha256((parent_job["id"] + json.dumps(
                    body, sort_keys=True, default=str)).encode()).hexdigest()
                self.state.save_observed_page(page_id, parent_job["id"], operation,
                                              "native_ui", body)
                pages.append(page_id)
            reply_parents = set(capture.reply_parents())
            reply_parents.update(
                str(parent) for parent in capture.native_latest
                if parent != "parents" and
                capture.native_versions.get(str(parent), 0) > versions.get(str(parent), 0))
            for parent in sorted(reply_parents):
                reply_job = self._ensure_reply_job(parent_job, parent, capture)
                latest = capture.native_latest.get(str(parent))
                fresh = capture.native_versions.get(str(parent), 0) > versions.get(str(parent), 0)
                response_pages = max(
                    0, capture.native_versions.get(str(parent), 0) -
                    versions.get(str(parent), 0))
                exhausted = bool(fresh and latest and latest[0].get("observedHasNext") is False)
                rows = capture.reply_rows(parent)
                rest_page = (capture.reply_latest_pages.get(str(parent)) or {}
                             if fresh and latest and
                             latest[0].get("transport") == "rest" else {})
                body = {**{key: value for key, value in rest_page.items()
                           if key != "child_comments"},
                        "child_comments": rows, "_uiObservation": {
                    "receivedAt": capture._native_times.get(str(parent)),
                    "freshResponse": fresh,
                    "responsePages": response_pages,
                    "observedCursor": latest[0].get("observedCursor") if latest else None,
                    "hasNextPage": latest[0].get("observedHasNext") if latest else None,
                    "endCursor": latest[0].get("observedEndCursor") if latest else None,
                    "exhaustionConfirmed": exhausted}}
                signature = json.dumps(rows, sort_keys=True, default=str)
                page_id = hashlib.sha256((reply_job["id"] + signature + str(
                    body["_uiObservation"])).encode()).hexdigest()
                self.state.save_observed_page(page_id, reply_job["id"], operation,
                                              "native_ui", body)
                pages.append(page_id)
            capture.commit_claims(self.state)
        capture._checkpoint_versions = dict(capture.native_versions)
        for read_operation in self._observer_pending_operations:
            self.state.update_operation(read_operation, "applied", phase="pages_saved", last_status="done")
        self._observer_pending_operations.clear()
        for page_id in pages:
            row = next((p for p in self.state.unapplied_observed_pages()
                        if p["id"] == page_id), None)
            if row:
                self._apply_observed_page(row)
        if not pages:
            self.state.update_operation(operation, "applied", phase="har_empty",
                                        last_status="done")
        capture.comments.clear()
        for parent in list(capture.reply_parents()):
            capture.clear_reply_rows(parent)
        self._observer_ack(parent_job["id"])
        saved_parent = self.state.get_job(parent_job["id"])
        if saved_parent and parent_job["kind"] == "parents":
            for key in ("parentNetworkTerminal", "uiEndCursor", "pages", "dataPages",
                        "parentPageSizes", "recoveryObservedParents",
                        "parentDomCheckQueue"):
                if key in saved_parent["payload"]:
                    parent_job["payload"][key] = saved_parent["payload"][key]
        if self.observer is not None and self.observer.dropped:
            self._fall_back_to_har("observer_dropped_events")
        elif self.observer is not None and self.observer.errors:
            self._fall_back_to_har("observer_copy_errors")
        return len(pages)

    def _apply_saved_observations(self):
        for page in list(self.state.unapplied_observed_pages()):
            self._apply_observed_page(page)

    def apply_saved_pages(self):
        """Commit decoded work before resolving or submitting browser work."""
        self._apply_saved_observations()
        rows = list(self.state.db.execute(
            "SELECT DISTINCT job_id FROM pending_pages WHERE applied_at IS NULL"))
        for row in rows:
            job = self.state.get_job(row[0])
            if job:
                self._page(job)

    # ---------------------------------------------------------- traversal --

    def _capture_native(self, scope, job):
        """Open the capture for this post and drive the traversal until the
        UI's parent query has been observed (or the list cannot advance)."""
        capture = NetworkCapture(
            media_id=scope, newest_first=self.config.is_newest_comments,
            stop_on_rejection=True,
            rejection_since=getattr(self.ctx, "authenticated_since", None),
            fingerprint_store=self.state, defer_claims=True)
        versions = dict(capture.native_versions)
        # The initial load is covered by one full dump: the observer only
        # sees requests made after it is installed.
        if self.observer is not None:
            self._install_observer(job["id"])
        elif self._witness is not None:
            self._install_witness(job["id"])
        operation = self._har_poll(capture, job["id"], "initial")
        self._checkpoint_capture(capture, job, operation, versions)
        if self.observer is not None or self._witness is not None:
            self._initial_snapshot_ids = set(capture.seen_comment_ids)
        ui_actions = []
        payload = job["payload"]
        while True:
            self._budget()
            if "parents" in capture.native:
                ui_actions.append({"action": "parents_observed_before_scroll",
                                   "phase": payload.get("uiPhase")})
                break
            action, _, _, decision, threads = self._ui_phase_step(capture, job, preload=True)
            action = {**action, "threads": threads}
            ui_actions.append(action)
            if decision in self.UI_STOPS:
                payload["uiPreloadStopReason"] = decision
                self.state.save_job(job)
                break
        return capture, ui_actions

    def _ui_phase_step(self, capture, job, *, preload=False):
        """One step of the traversal state machine.

        scan_segment  -- examine the screen (parent rows, reply controls),
                         drain the threads on it, move 70% forward
        load_parents  -- press "Load more comments" / reach the bottom, wait
                         for Instagram's next page
        restore_anchor -- go back to the last examined row (one screen of
                         overlap) so the new segment is scanned, not skipped
        audit_gaps    -- classify and revisit parent IDs left unscanned after
                         the terminal network page
        """
        payload, scope = job["payload"], job["scope"]
        immediate_checks = []
        initial_check = self._check_parent_dom_queue(job)
        if initial_check:
            immediate_checks.append(initial_check)
        phase = payload.get("uiPhase")
        if phase not in ("scan_segment", "load_parents", "restore_anchor", "audit_gaps"):
            phase = ("restore_anchor" if payload.get("lastScannedId") and
                     payload.get("uiRecovering") else "scan_segment")
        payload["uiPhase"] = phase
        previous_version = capture.native_versions.get("parents", 0)
        threads = None
        if phase == "scan_segment":
            action, operation, decision, threads = self._scan_segment_step(capture, job)
        elif phase == "restore_anchor":
            action, operation, decision = self._restore_anchor_step(capture, job)
        elif phase == "audit_gaps":
            action, operation, decision, threads = self._audit_gap_step(capture, job)
        else:
            action, operation, previous_version, decision = self._parent_ui_step(
                capture, job, f"parent:{scope}:{payload.get('uiSteps', 0)}",
                wait=3.0 if preload else 6.0)
            if (decision == "ui_pagination_unresponsive"
                    and not preload and payload.get("gapAuditAfterStop") is None
                    and self.state.unscanned_parent_ids(scope)):
                # The list ended (at the bottom, pagination silent) without a
                # server terminal. The audit that classifies the unscanned
                # ids -- and hands never-rendered threads to the permalink
                # route -- runs here as well; the UI stop is re-applied once
                # it is done, so the parent list is not declared complete.
                payload["gapAuditAfterStop"] = decision
                payload["gapAuditQueue"] = sorted(
                    self.state.unscanned_parent_ids(scope),
                    key=lambda parent: self._gap_audit_priority(scope, parent), reverse=True)
                payload["uiPhase"] = "audit_gaps"
                payload["gapAuditStartedAt"] = time.time()
                payload.pop("stopReason", None)
                action = {**action, "action": "gap_audit_started_after_ui_stop",
                          "uiStop": decision, "remaining": len(payload["gapAuditQueue"])}
                decision = "continue"
            elif capture.native_versions.get("parents", 0) > previous_version:
                payload["uiPhase"] = "restore_anchor"
                if payload.get("uiRecovering"):
                    self.state.add_invocation_metric("recoveryReloadPages", 1)
                    payload["recoveryReloads"] = payload.get("recoveryReloads", 0) + 1
                    payload["recoveryLastProgressReload"] = payload["recoveryReloads"]
                    payload["recoveryLastProgressCursor"] = payload.get("uiEndCursor")
        final_check = self._check_parent_dom_queue(job)
        if final_check:
            immediate_checks.append(final_check)
        if immediate_checks:
            action["parentDomChecks"] = immediate_checks
        action["phase"] = phase
        action["nextPhase"] = payload.get("uiPhase")
        self.state.save_job(job)
        if (self.observer is not None and phase == "load_parents"
                and not self._observer_saw["parents"] and
                (decision in self.UI_STOPS or payload.get("uiSteps", 0) >= 4)):
            # The observer saw no parent page where the UI paginated: the
            # archive decides. One filtered dump now; a page found there
            # overrides a stop decided on the observer's silence.
            self._fall_back_to_har("observer_no_parent_page")
            versions = dict(capture.native_versions)
            operation = self._har_poll(capture, job["id"], "fallback_parents")
            self._checkpoint_capture(capture, job, operation, versions)
            if capture.native_versions.get("parents", 0) > previous_version and decision in self.UI_STOPS:
                decision = "continue"
                payload.pop("uiPreloadStopReason", None)
                self.state.save_job(job)
        return action, operation, previous_version, decision, threads

    def _check_parent_dom_queue(self, job):
        payload, scope = job["payload"], job["scope"]
        queue = [str(v) for v in (payload.get("parentDomCheckQueue") or [])]
        if not queue:
            return None
        batch = queue[:200]
        result, _ = self._run_ui_command(
            job["id"], "parent_dom_check",
            f"parent_dom:{scope}:{payload.get('pageLoadAttempts', 0)}:{len(queue)}",
            lambda event: native_comments.inspect_parent_ids(
                self.ctx.session, batch, known_parents=self.state.parent_order(scope),
                on_event=event))
        summary = {"checked": len(batch), "action": result.get("action"),
                   "at": time.time()}
        if result.get("action") == "parents_inspected":
            observations = [row for row in (result.get("parents") or [])
                            if isinstance(row, dict)]
            self.state.mark_parent_dom_observations(
                scope, observations, document=payload.get("pageLoadAttempts", 0),
                stage="immediate")
            present = [row for row in observations if row.get("present")]
            visible = [row for row in observations if row.get("visible")]
            controls = [row for row in observations if row.get("control")]
            summary.update(
                present=len(present), visible=len(visible), controls=len(controls),
                absentSample=[str(row.get("parentId")) for row in observations
                              if not row.get("present")][:20])
            payload["parentDomCheckQueue"] = queue[len(batch):]
            self.state.add_invocation_metric("immediateParentChecks", len(observations))
            self.state.add_invocation_metric("immediateParentsPresent", len(present))
        else:
            payload["parentDomCheckFailures"] = payload.get("parentDomCheckFailures", 0) + 1
        history = list(payload.get("parentDomCheckLog") or [])
        history.append(summary)
        payload["parentDomCheckLog"] = history[-100:]
        self.state.save_job(job)
        return summary

    def _region_unavailable_step(self, capture, job, listing):
        """The screen has no verified comment region right now. Reuse the
        bounded observation-only recovery of the load step, which ends in
        ``ui_region_unavailable`` if the list never renders."""
        payload, scope = job["payload"], job["scope"]
        action, operation, _, decision = self._parent_ui_step(
            capture, job, f"region:{scope}:{payload.get('uiSteps', 0)}", wait=3.0)
        action["listing"] = listing.get("action")
        return action, operation, decision

    def _scan_segment_step(self, capture, job):
        payload, scope = job["payload"], job["scope"]
        known = self.state.parent_order(scope)
        listing, _ = self._run_ui_command(
            job["id"], "thread_list",
            f"screen:{scope}:{payload.get('uiSteps', 0)}:{payload.get('scanSteps', 0)}",
            lambda event: native_comments.visible_thread_parents(
                self.ctx.session, known_parents=known, on_event=event))
        if listing.get("action") in ("no_comment_links", "no_verified_scroller",
                                     "unsupported_session", "invalid_result"):
            action, operation, decision = self._region_unavailable_step(capture, job, listing)
            return action, operation, decision, None
        structural = [str(p) for p in (listing.get("structuralParents") or [])
                      if str(p).isdigit()]
        if structural:
            self.state.append_parent_order(scope, structural, origin="structural")
            known = self.state.parent_order(scope)
        visible = [str(p) for p in (listing.get("visibleParents") or [])
                   if str(p).isdigit()]
        controls = {str(row.get("parentId")) for row in (listing.get("parents") or [])
                    if isinstance(row, dict) and row.get("parentId") is not None}
        present = [str(p) for p in (listing.get("presentParents") or []) if str(p).isdigit()]
        self.state.mark_parent_dom_observations(scope, [
            {"parentId": parent, "present": True, "visible": parent in visible,
             "control": parent in controls} for parent in present],
            document=payload.get("pageLoadAttempts", 0))
        self.state.mark_scanned(scope, visible)
        if visible:
            payload["lastScannedId"] = visible[-1]
        if listing.get("ambiguousControls"):
            self.state.add_invocation_metric("ambiguousControls",
                                             int(listing.get("ambiguousControls") or 0))
        threads = self._expand_threads_in_place(capture, job, listing=listing, known=known)
        payload["uiPhase"] = "scan_segment"
        if self.state.count(scope) >= self.config.results_limit:
            self.limited_scopes.add(scope)
            payload["stopReason"] = "results_limit"
            return {"action": "results_limit"}, None, "continue", threads
        if listing.get("atEnd") and payload.get("parentNetworkTerminal"):
            unscanned = self.state.unscanned_parent_ids(scope)
            if unscanned:
                payload["unscannedParentCount"] = len(unscanned)
                payload["unscannedParentSample"] = unscanned[:20]
                payload["gapAuditQueue"] = sorted(
                    unscanned, key=lambda parent: self._gap_audit_priority(scope, parent),
                    reverse=True)
                payload["uiPhase"] = "audit_gaps"
                payload["gapAuditStartedAt"] = time.time()
                payload.pop("stopReason", None)
                return {"action": "gap_audit_started", "atEnd": True,
                        "remaining": len(unscanned)}, None, "continue", threads
            payload["scanComplete"] = True
            return {"action": "segment_scanned", "atEnd": True}, None, "continue", threads
        scanned = self.state.scanned_parent_ids(scope)
        after = present.index(visible[-1]) + 1 if visible and visible[-1] in present else None
        # Without a visible row there is no trustworthy forward boundary.
        # Starting at present[0] caused a measured 44864 -> 428 px jump.
        target = (next((p for p in present[after:] if p not in scanned), None)
                  if after is not None else None)
        if target:
            action, _ = self._run_ui_command(
                job["id"], "scan_to_parent",
                f"scan_to:{scope}:{target}:{payload.get('scanSteps', 0)}",
                lambda event: native_comments.scan_to_parent(
                    self.ctx.session, target, wait=2.0, on_event=event))
            before_top = action.get("before")
            settled_top = action.get("settledTop", action.get("after"))
            forward = (action.get("action") == "advanced_to_parent" and
                       isinstance(before_top, (int, float)) and
                       isinstance(settled_top, (int, float)) and
                       settled_top > before_top + 2)
            if forward:
                self.state.add_invocation_metric("targetedParentMoves", 1)
            else:
                target = None
        if not target:
            action, _ = self._run_ui_command(
                job["id"], "scan_step", f"scan:{scope}:{payload.get('scanSteps', 0)}",
                lambda event: native_comments.scan_step(self.ctx.session, wait=3.0, on_event=event))
        payload["scanSteps"] = payload.get("scanSteps", 0) + 1
        self.state.add_invocation_metric("scanSteps", 1)
        versions = dict(capture.native_versions)
        operation = self._poll_capture(capture, job["id"], f"scan:{payload['scanSteps']}")
        self._checkpoint_capture(capture, job, operation, versions)
        if action.get("action") in ("no_comment_links", "no_verified_scroller"):
            fallback, operation, decision = self._region_unavailable_step(capture, job, action)
            return fallback, operation, decision, threads
        grew = capture.native_versions.get("parents", 0) > versions.get("parents", 0)
        if grew:
            # A page Instagram loaded on its own while we scanned: already
            # saved by the checkpoint above; the region simply got longer.
            payload["autoLoadedPages"] = payload.get("autoLoadedPages", 0) + 1
        at_end = bool(action.get("settledAtEnd") if "settledAtEnd" in action else action.get("atEnd"))
        progress = {"uiProgress": payload.setdefault("scanProgress", {})}
        assessment = self._assess_parent_movement(progress, {**action, "action": "scrolled_comment_region"})
        decision = "continue"
        if not at_end and not grew and not assessment["moved"]:
            for look in range(2):
                self._budget()
                check, _ = self._run_ui_command(job["id"], "parent_settle",
                    f"scan_settle:{scope}:{payload['scanSteps']}:{look}",
                    lambda event: native_comments.observe_ui(self.ctx.session, wait=5.0, on_event=event))
                check_versions = dict(capture.native_versions)
                operation = self._poll_capture(capture, job["id"], "scan_settle")
                self._checkpoint_capture(capture, job, operation, check_versions)
                assessment = self._assess_parent_movement(progress, check)
                if assessment["moved"] or capture.native_versions.get("parents", 0) > versions.get("parents", 0):
                    break
            else:
                decision = "ui_scroll_stalled" if assessment["regionAvailable"] else "ui_region_unavailable"
        history = list(payload.get("scanStepDiagnostics") or [])
        history.append({"step": payload["scanSteps"], **assessment, "decision": decision})
        payload["scanStepDiagnostics"] = history[-100:]
        if at_end and not grew:
            # The next call must inspect the bottom viewport before loading.
            payload["uiPhase"] = "scan_segment" if not listing.get("atEnd") else "load_parents"
        action.update(visibleParents=len(visible), lastScannedId=payload.get("lastScannedId"),
                      autoLoaded=grew)
        return action, operation, decision, threads

    def _gap_audit_priority(self, scope, parent):
        declared = self._thread_declared_count(scope, parent) or 0
        job = self.state.get_job(f"replies:{scope}:{parent}")
        saved = self._thread_count(job) if job else 0
        return max(0, declared - saved), declared

    def _mark_parent_not_rendered(self, scope, parent):
        reply = self.state.get_job(f"replies:{scope}:{parent}")
        if (reply and reply["status"] != "done" and
                not reply['payload'].get('serverExhaustionConfirmed')):
            invocation = self.state.get_meta("invocation", 1)
            if reply["payload"].get("directUiAttemptedInvocation") == invocation:
                reply["payload"]["stopReason"] = "parent_not_rendered_in_dom"
                self.state.save_job(reply, status="blocked")
            else:
                reply["payload"].update(
                    source="direct_ui", queue=[{}], seenCursors=[],
                    stopReason="direct_ui_pending")
                self.state.save_job(reply, status="pending")
                self.state.add_invocation_metric("directUiQueued", 1)

    def _direct_parent_thread(self, job):
        payload, scope = job["payload"], job["scope"]
        parent = str(payload["parentId"])
        invocation = self.state.get_meta("invocation", 1)
        if payload.get("directUiAttemptedInvocation") == invocation:
            payload["stopReason"] = "direct_ui_already_attempted"
            self.state.save_job(job, status="blocked")
            return
        payload["directUiAttemptedInvocation"] = invocation
        payload["directUiStartCount"] = self._thread_count(job)
        payload["stopReason"] = "direct_ui_opening"
        self.state.save_job(job)
        url = f"{post_url(payload['code'])}c/{parent}/"
        landed = self.ctx.goto(url, wait=8, refresh_tokens=True)
        if not self.ctx.authenticated:
            raise FatalError("Direct comment permalink rejected authenticated access")
        current = landed or self.ctx.current_page
        if f"/c/{parent}/" not in str(current):
            payload["stopReason"] = "direct_ui_redirected"
            self.state.save_job(job, status="blocked")
            return
        # A verified navigation created a new document. A waiting click from
        # the previous document cannot produce a response here. Unresolved
        # getbro commands are resolved by the runner before navigation.
        thread = self.state.thread_state(scope, parent)
        if thread and thread.get('phase') in ('waiting_response', 'click_submitting'):
            thread.update(previousDocumentPhase=thread['phase'],
                          phase='visible', last='direct_document_reopened')
            self.state.save_thread_state(scope, parent, thread)
        capture = NetworkCapture(
            media_id=scope, newest_first=self.config.is_newest_comments,
            stop_on_rejection=True,
            rejection_since=getattr(self.ctx, "authenticated_since", None),
            fingerprint_store=self.state, defer_claims=True)
        parent_job = self.state.get_job(f"parents:{scope}")
        operation = self._har_poll(capture, job["id"], f"direct_initial:{parent}")
        self._checkpoint_capture(capture, parent_job, operation, {})
        if self.observer is not None:
            self._install_observer(job["id"])
        elif self._witness is not None:
            self._install_witness(job["id"])
        inspection, _ = self._run_ui_command(
            job['id'], 'parent_dom_check', f'direct_inspect:{scope}:{parent}',
            lambda event: native_comments.inspect_parent_ids(
                self.ctx.session, [parent], known_parents=[parent], on_event=event))
        # Capture application may have advanced this reply's cursor; never
        # overwrite its newly saved payload with the pre-navigation snapshot.
        job = self.state.get_job(job['id'])
        payload = job['payload']
        row = next(iter(inspection.get("parents") or []), {})
        payload["directUiInitial"] = {
            "present": bool(row.get("present")), "visible": bool(row.get("visible")),
            "control": bool(row.get("control")),
            "advertisedCount": row.get("advertisedCount"),
            "renderedLinks": row.get("renderedLinks"),
            "path": inspection.get("path"), "links": inspection.get("links"),
            "scrollHeight": inspection.get("scrollHeight"),
            "landed": str(current)[:160]}
        self.state.add_invocation_metric("directUiAttempts", 1)
        self.state.save_job(job, status=job['status'])
        if not row.get("present"):
            # Keep what the page looked like: a fresh session renders these
            # permalinks, so an absent parent here is a fact about this
            # document, and the next diagnosis needs it.
            try:
                payload["directUiInitial"]["document"] = json.loads(self.ctx.session.js(
                    "JSON.stringify({url:location.href,title:document.title,"
                    "commentLinks:document.querySelectorAll('a[href*=\"/c/\"]').length,"
                    "postLinks:document.querySelectorAll('a[href*=\"/p/\"]').length,"
                    "text:(document.body&&document.body.innerText||'').slice(0,240)})",
                    out_type="str") or "{}")
            except (BroCommandError, BroPayloadError, ValueError, TypeError):
                pass
            payload["stopReason"] = "direct_ui_parent_missing"
            self.state.save_job(job, status="blocked")
            return
        if not row.get("control"):
            payload["stopReason"] = "direct_ui_control_absent"
            self.state.save_job(job, status="blocked")
            return
        result = self._drain_one_thread(
            capture, parent_job, parent, [parent], allow_dom_progress=True)
        current_job = self.state.get_job(job["id"])
        current_job["payload"]["directUiResult"] = result
        if (result.get("last") == "direct_ui_rest_handoff" and
                current_job["payload"].get("source") == "api" and
                current_job["payload"].get("queue")):
            current_job["payload"]["stopReason"] = "direct_ui_rest_handoff"
            self.state.save_job(current_job, status="pending")
        elif current_job["status"] != "done":
            if current_job['payload'].get('stopReason') != 'reported_count_gap':
                current_job["payload"]["stopReason"] = result.get("last") or "direct_ui_incomplete"
            self.state.save_job(current_job, status="blocked")
        else:
            self.state.save_job(current_job, status="done")
        self.state.add_invocation_metric("directUiReplies", max(
            0, self._thread_count(current_job) - int(payload.get("directUiStartCount") or 0)))

    def _finish_gap_audit(self, job):
        payload, scope = job["payload"], job["scope"]
        unresolved = self.state.unresolved_parent_ids(scope)
        diagnostics = self.state.parent_scan_diagnostics(scope)
        payload["gapAuditSummary"] = {
            **diagnostics, "unresolvedSample": unresolved[:20],
            "completedAt": time.time()}
        payload.pop("gapAuditQueue", None)
        if unresolved:
            payload["unscannedParentCount"] = len(unresolved)
            payload["unscannedParentSample"] = unresolved[:20]
            payload["stopReason"] = "scan_gaps"
            self.state.save_job(job)
            return {"action": "scan_gaps", "remaining": len(unresolved)}, None, "scan_gaps", None
        ui_stop = payload.pop("gapAuditAfterStop", None)
        if ui_stop:
            # Audited after a UI stop: the never-rendered threads are queued
            # for the permalink route, but the list itself stays where the
            # UI left it -- unresponsive pagination is not completeness.
            payload["stopReason"] = ui_stop
            payload["uiPhase"] = "load_parents"
            self.state.save_job(job)
            return {"action": "gap_audit_complete_after_ui_stop", "uiStop": ui_stop,
                    **diagnostics}, None, ui_stop, None
        payload["scanComplete"] = True
        payload["uiPhase"] = "scan_segment"
        payload.pop("stopReason", None)
        self.state.save_job(job)
        return {"action": "gap_audit_complete", **diagnostics}, None, "continue", None

    def _audit_gap_step(self, capture, job):
        payload, scope = job["payload"], job["scope"]
        queue = [str(v) for v in (payload.get("gapAuditQueue") or [])]
        if not queue:
            return self._finish_gap_audit(job)
        known = self.state.parent_order(scope)
        batch = queue[:200]
        result, _ = self._run_ui_command(
            job["id"], "gap_audit_inspect",
            f"gap_inspect:{scope}:{payload.get('gapAuditSteps', 0)}:{len(queue)}",
            lambda event: native_comments.inspect_parent_ids(
                self.ctx.session, batch, known_parents=known, on_event=event))
        if result.get("action") != "parents_inspected":
            payload["stopReason"] = "ui_region_unavailable"
            self.state.save_job(job)
            return result, None, "ui_region_unavailable", None
        observations = [row for row in (result.get("parents") or []) if isinstance(row, dict)]
        self.state.mark_parent_dom_observations(
            scope, observations, document=payload.get("pageLoadAttempts", 0),
            stage="terminal")
        status_rows = {row["parent_id"]: row for row in self.state.db.execute(
            "SELECT parent_id,dom_status,present_at FROM ui_parent_order WHERE scope=?",
            (str(scope),))}
        remove = set()
        for row in observations:
            parent = str(row.get("parentId") or "")
            saved = status_rows.get(parent)
            if row.get("visible"):
                continue
            if not row.get("present") and saved and saved["dom_status"] == "not_rendered":
                remove.add(parent)
                self._mark_parent_not_rendered(scope, parent)
            elif not row.get("present") and saved and saved["present_at"] is not None:
                self.state.mark_parent_focus_failed(scope, parent)
                remove.add(parent)
        visible = [row for row in observations if row.get("visible")]
        threads = None
        if visible:
            visible_ids = [str(row["parentId"]) for row in visible]
            self.state.mark_scanned(scope, visible_ids)
            listing = {
                "action": "threads_visible" if any(row.get("control") for row in visible)
                else "no_thread_controls",
                "parents": [{"parentId": str(row["parentId"]),
                             "text": row.get("controlText"),
                             "advertisedCount": row.get("advertisedCount"),
                             "ownership": "local_unique"}
                            for row in visible if row.get("control")],
                "visibleParents": visible_ids,
                "presentParents": [str(row["parentId"]) for row in observations
                                   if row.get("present")],
                "ambiguousControls": 0}
            threads = self._expand_threads_in_place(capture, job, listing=listing, known=known)
            remove.update(visible_ids)
            self.state.add_invocation_metric("gapAuditParentsScanned", len(visible_ids))
        remaining_present = [row for row in observations
                             if row.get("present") and not row.get("visible") and
                             str(row.get("parentId")) not in remove]
        operation = None
        action = {"action": "gap_audit_classified", "checked": len(observations)}
        if not visible and remaining_present:
            target = max(remaining_present,
                         key=lambda row: self._gap_audit_priority(scope, str(row["parentId"])))
            parent = str(target["parentId"])
            attempts = self.state.add_parent_focus_attempt(scope, parent)
            if attempts <= 2:
                action, _ = self._run_ui_command(
                    job["id"], "gap_audit_focus",
                    f"gap_focus:{scope}:{parent}:{attempts}",
                    lambda event: native_comments.focus_parent(
                        self.ctx.session, parent, wait=2.0, on_event=event))
                versions = dict(capture.native_versions)
                operation = self._poll_capture(capture, job["id"], f"gap_focus:{parent}")
                self._checkpoint_capture(capture, job, operation, versions)
                self.state.add_invocation_metric("gapAuditFocusMoves", 1)
                if action.get("action") != "audit_parent_focused":
                    self.state.mark_parent_focus_failed(scope, parent)
                    remove.add(parent)
            else:
                self.state.mark_parent_focus_failed(scope, parent)
                remove.add(parent)
        queue = [parent for parent in queue if parent not in remove]
        payload["gapAuditQueue"] = queue
        payload["gapAuditSteps"] = payload.get("gapAuditSteps", 0) + 1
        payload["gapAuditLast"] = {"action": action.get("action"),
                                   "remaining": len(queue),
                                   "checked": len(observations)}
        self.state.save_job(job)
        if not queue:
            completed = self._finish_gap_audit(job)
            if threads is not None:
                return completed[0], completed[1], completed[2], threads
            return completed
        return action, operation, "continue", threads

    def _restore_anchor_step(self, capture, job):
        payload, scope = job["payload"], job["scope"]
        anchor = payload.get("lastScannedId")
        if not anchor:
            payload["uiPhase"] = "scan_segment"
            payload.pop("uiRecovering", None)
            return {"action": "no_anchor"}, None, "continue"
        order = self.state.parent_order(scope)
        index = order.index(anchor) if anchor in order else -1
        fallbacks = list(reversed(order[max(0, index - 40):index])) if index > 0 else []
        action, _ = self._run_ui_command(
            job["id"], "anchor_restore", f"anchor:{scope}:{anchor}:{payload.get('uiSteps', 0)}",
            lambda event: native_comments.restore_anchor(
                self.ctx.session, anchor, fallbacks, wait=2.0, on_event=event))
        self.state.add_invocation_metric("anchorRestores", 1)
        versions = dict(capture.native_versions)
        operation = self._poll_capture(capture, job["id"], f"anchor:{payload.get('uiSteps', 0)}")
        self._checkpoint_capture(capture, job, operation, versions)
        outcome = action.get("action")
        if outcome == "anchored":
            if action.get("usedId") != anchor:
                self.state.add_invocation_metric("anchorFallbacks", 1)
            payload["uiPhase"] = "scan_segment"
            payload.pop("uiRecovering", None)
            return action, operation, "continue"
        if outcome == "anchor_missing":
            reloads = payload.get("recoveryReloads", 0)
            limit = self._recovery_page_limit(payload, scope, anchor)
            payload["recoveryPageLimit"] = limit
            delivered = {str(v) for v in (payload.get("recoveryObservedParents") or [])}
            anchor_position = int(payload.get("recoveryAnchorPosition") or 0)
            neighbours = {str(v) for v in (payload.get("recoveryAnchorNeighbors") or [])}
            if (payload.get("uiRecovering") and anchor_position > 0 and
                    len(delivered) >= anchor_position):
                seen_candidates = ({anchor} | neighbours) & delivered
                reason = ("anchor_evicted_from_dom" if seen_candidates else
                          "anchor_not_in_ranked_pass")
                self._record_recovery_gap(
                    payload, scope, anchor, action, reason,
                    delivered_count=len(delivered),
                    anchor_position=anchor_position,
                    seen_candidates=sorted(seen_candidates))
                payload["lastScannedId"] = None
                payload["uiPhase"] = "scan_segment"
                payload["recoveryExitReason"] = reason
                payload.pop("uiRecovering", None)
                self.state.add_invocation_metric(
                    "recoveryAnchorsEvicted" if seen_candidates else
                    "recoveryAnchorsAbsent", 1)
                self.state.save_job(job)
                return action, operation, "continue"
            progressed = payload.get("recoveryLastProgressReload") == reloads
            if (payload.get("uiRecovering") and not payload.get("parentNetworkTerminal")
                    and (reloads < limit or progressed)):
                if reloads >= limit and progressed:
                    # The estimate is a diagnostic guard, not a hard ceiling.
                    # Productive pages/cursors keep recovery alive.
                    limit = reloads + 2
                    payload["recoveryPageLimit"] = limit
                    self.state.add_invocation_metric("recoveryEstimateExtensions", 1)
                # A fresh document: the examined rows are not loaded yet.
                # Reload pages (already saved, never re-recorded) until the
                # anchor or a neighbour of it is back, then scan on.
                payload["uiPhase"] = "load_parents"
                self.state.save_job(job)
                return action, operation, "continue"
            self._record_recovery_gap(
                payload, scope, anchor, action, "anchor_missing",
                delivered_count=len(delivered), anchor_position=anchor_position)
            payload["lastScannedId"] = None
            payload["uiPhase"] = "scan_segment"
            payload.pop("uiRecovering", None)
            self.state.save_job(job)
            return action, operation, "continue"
        fallback, operation, decision = self._region_unavailable_step(capture, job, action)
        return fallback, operation, decision

    def _record_recovery_gap(self, payload, scope, anchor, action, reason,
                             *, delivered_count=0, anchor_position=0,
                             seen_candidates=()):
        gaps = list(payload.get("scanGaps") or [])
        unscanned = self.state.unscanned_parent_ids(scope)
        gaps.append({"from": anchor, "reason": reason,
                     "presentLinks": action.get("present"),
                     "deliveredCurrentDocument": int(delivered_count),
                     "anchorPosition": int(anchor_position),
                     "seenAnchorCandidates": list(seen_candidates)[:20],
                     "unscannedCount": len(unscanned),
                     "unscannedFirst": unscanned[0] if unscanned else None,
                     "unscannedLast": unscanned[-1] if unscanned else None,
                     "unscannedSample": unscanned[:20],
                     "step": payload.get("uiSteps", 0)})
        payload["scanGaps"] = gaps[-200:]

    def _recovery_page_limit(self, payload, scope, anchor):
        """Pages needed to restore an anchor, based on durable parent order."""
        order = self.state.parent_order(scope)
        position = order.index(anchor) + 1 if anchor in order else len(order)
        sizes = [int(v) for v in (payload.get("parentPageSizes") or []) if int(v) > 0]
        if not sizes:
            parent_job = self.state.db.execute(
                "SELECT id FROM jobs WHERE kind='parents' AND scope=? LIMIT 1",
                (str(scope),)).fetchone()
            if parent_job:
                for row in self.state.db.execute(
                        "SELECT payload FROM observed_pages WHERE job_id=? ORDER BY created_at",
                        (parent_job[0],)):
                    body = json.loads(row[0])
                    observation = body.get("_parentObservation") or {}
                    pages = max(0, int(observation.get("responsePages") or 0))
                    rows = body.get("comments") or []
                    if pages and rows:
                        estimated = max(1, (len(rows) + pages - 1) // pages)
                        sizes.extend([estimated] * pages)
            if sizes:
                payload["parentPageSizes"] = sizes[-100:]
        completed_pages = max(1, int(payload.get("totalPages", 0)) +
                              int(payload.get("dataPages", 0)))
        inferred = max(1, round(len(order) / completed_pages)) if order else 15
        # Terminal/overlap pages can be shorter; the median reflects the
        # regular pagination batch without letting one partial page inflate
        # the recovery estimate for the whole list.
        page_size = sorted(sizes)[len(sizes) // 2] if sizes else inferred
        required = (position + page_size - 1) // page_size
        return max(3, required + 2)

    def _begin_anchor_recovery(self, payload, scope):
        anchor = payload.get("lastScannedId")
        order = self.state.parent_order(scope)
        index = order.index(anchor) if anchor in order else -1
        neighbours = order[max(0, index - 40):index] if index >= 0 else []
        payload.update(
            uiRecovering=True, uiPhase="restore_anchor", recoveryReloads=0,
            recoveryObservedParents=[],
            recoveryAnchorPosition=index + 1 if index >= 0 else len(order),
            recoveryAnchorNeighbors=neighbours,
            recoveryPageLimit=self._recovery_page_limit(payload, scope, anchor))
        for key in ("recoveryLastProgressReload", "recoveryLastProgressCursor",
                    "recoveryExitReason"):
            payload.pop(key, None)

    # ------------------------------------------------------------ threads --

    def _thread_declared_count(self, scope, parent):
        record = self.state.get_record(f"comment:{scope}:{parent}")
        value = (record or {}).get("payload", {}).get("repliesCount")
        return value if isinstance(value, int) else None

    def _mark_thread_exhausted(self, scope, parent, state, reply_job):
        observation = reply_job["payload"].get("uiLastObservation") or {}
        state.update(
            phase="server_exhausted", last="server_exhausted",
            completedReplyCount=self._thread_count(reply_job),
            completedDeclaredCount=self._thread_declared_count(scope, parent),
            terminalEndCursor=observation.get("endCursor"),
            terminalObservedAt=observation.get("receivedAt") or time.time())
        self.state.save_thread_state(scope, parent, state)

    def _thread_has_new_evidence(self, capture, scope, parent, state, control=None):
        """Return why a completed/collapsed thread may be opened again."""
        current = self._thread_count(self.state.get_job(f"replies:{scope}:{parent}") or
                                     {"scope": scope, "payload": {"parentId": parent}})
        baseline = max(int(state.get("completedReplyCount") or current),
                       int(state.get("completedDeclaredCount") or 0))
        declared = self._thread_declared_count(scope, parent)
        advertised = control.get("advertisedCount") if isinstance(control, dict) else None
        if isinstance(declared, int) and declared > baseline:
            return "declared_count_increased"
        if isinstance(advertised, int) and advertised > baseline:
            return "visible_count_increased"
        for raw in capture.reply_rows(parent):
            ident = identity(raw) if isinstance(raw, dict) else None
            if ident and not self.state.exists("comment", scope, ident):
                return "new_reply_id"
        latest = capture.native_latest.get(str(parent))
        if latest and latest[0].get("transport") != "rest":
            template = latest[0]
            observed_at = capture._native_times.get(str(parent), 0)
            if (template.get("observedHasNext") is True and
                    template.get("observedEndCursor") != state.get("terminalEndCursor") and
                    observed_at > float(state.get("terminalObservedAt") or observed_at)):
                return "new_network_cursor"
        return None

    def _should_open_thread(self, capture, scope, parent, state, reply_job, control):
        evidence = self._thread_has_new_evidence(capture, scope, parent, state, control)
        if state.get("invalidatedLocatorVersion") == native_comments.LOCATOR_VERSION:
            return False, "invalidated_binding"
        completed = (state.get("phase") == "server_exhausted" or
                     reply_job and reply_job["status"] == "done" and
                     reply_job["payload"].get("uiExhaustionConfirmed"))
        if completed:
            return bool(evidence), evidence or "completed_without_new_evidence"
        saved = self._thread_count(reply_job) if reply_job else 0
        declared = self._thread_declared_count(scope, parent)
        advertised = control.get("advertisedCount") if isinstance(control, dict) else None
        if state.get("invocation") != self.state.get_meta("invocation", 1) and saved:
            target = max(v for v in (declared, advertised, saved) if isinstance(v, int))
            if target <= saved and not evidence:
                return False, "known_replies_already_saved"
        return True, evidence

    def _thread_unfinished(self, scope, parent):
        """A visible parent whose thread still owes replies: a reply job
        exists (positive count, previews or a confirmed reply created it)
        and neither the server nor this pass has closed it."""
        job = self.state.get_job(f"replies:{scope}:{parent}")
        if not job or job["status"] == "done":
            return False
        if job["payload"].get("stopReason") in ("reply_click_cap", "reply_limit", "results_limit"):
            return False
        state = self.state.thread_state(scope, parent) or {}
        invocation = self.state.get_meta("invocation", 1)
        if state.get("invocation") == invocation and state.get("last") in (
                "ui_control_absent", "ambiguous_thread_boundary", "click_cap", "misattributed"):
            return False
        return state.get("phase") != "server_exhausted"

    def _await_thread_response(self, capture, reply_job, parent, versions):
        """After a click, read until this thread's response arrives -- a
        bounded number of looks, never another click."""
        operation = self._poll_capture(capture, reply_job["id"],
                                       f"reply:{parent}:{versions.get(str(parent), 0) + 1}")
        parent_job = self.state.get_job(f"parents:{reply_job['scope']}")
        if parent_job:
            self._checkpoint_capture(capture, parent_job, operation, versions)
        for look in range(self.REPLY_RESPONSE_LOOKS):
            if capture.native_versions.get(str(parent), 0) > versions.get(str(parent), 0):
                break
            self._budget()
            self._run_ui_command(
                reply_job["id"], "reply_wait",
                f"reply_wait:{reply_job['id']}:{versions.get(str(parent), 0)}:{look}",
                lambda event: native_comments.observe_ui(self.ctx.session, wait=2.0, on_event=event))
            operation = self._poll_capture(capture, reply_job["id"],
                                           f"reply:{parent}:{versions.get(str(parent), 0) + 1}:{look + 1}")
            if parent_job:
                self._checkpoint_capture(capture, parent_job, operation, versions)
        return operation

    def _drain_one_thread(self, capture, parent_job, parent, others, *, allow_dom_progress=False):
        """Checkpoint one reply response after every successful UI click."""
        reply_job = self._ensure_reply_job(parent_job, parent)
        scope = parent_job["scope"]
        invocation = self.state.get_meta("invocation", 1)
        state = self.state.thread_state(scope, parent) or {
            "phase": "visible", "clicks": 0}
        if state.get("phase") in ("waiting_response", "click_submitting"):
            # The previous attempt ended after a click and before its
            # response was read (a crash, a lost command). The control was
            # pressed: consume what the page holds before pressing anything.
            versions = dict(capture.native_versions)
            operation = self._await_thread_response(capture, reply_job, parent, versions)
            self._checkpoint_capture(capture, parent_job, operation, versions)
            current = self.state.get_job(reply_job["id"])
            received = ((current or {}).get("payload", {}).get("uiLastObservation") or {}).get("receivedAt")
            if (capture.native_versions.get(str(parent), 0) <= versions.get(str(parent), 0)
                    and not (received and received >= state.get("clickedAt", float('inf')))):
                state.update(phase="deferred", last="reply_response_missing")
                self.state.save_thread_state(scope, parent, state)
                return {"parentId": parent, "clicks": state.get("clicks", 0), "last": state["last"]}
            state.update(phase="visible", last="response_recovered")
            self.state.save_thread_state(scope, parent, state)
            self.state.add_invocation_metric("recoveredClickResponses", 1)
            if current and current["payload"].get("uiExhaustionConfirmed"):
                self._mark_thread_exhausted(scope, parent, state, current)
                return {"parentId": parent, "clicks": state.get("clicks", 0),
                        "last": "server_exhausted"}
        if state.get("invocation") != invocation:
            state.update(invocation=invocation, clicksThisInvocation=0,
                         absentChecks=0, phase="visible")
        for _ in range(max(0, self.THREAD_DRAIN_CLICKS - state.get("clicksThisInvocation", 0))):
            self._budget()
            current = self.state.get_job(reply_job["id"])
            limit_reason = None
            if self.state.count(scope) >= self.config.results_limit:
                self.limited_scopes.add(scope)
                limit_reason = "results_limit"
            elif current["payload"].get("dataPages", 0) >= self.config.max_comment_pages:
                limit_reason = "page_limit"
            elif (self.config.max_replies_per_comment is not None and
                  self._thread_count(reply_job) >= self.config.max_replies_per_comment):
                limit_reason = "reply_limit"
            if limit_reason:
                current["payload"]["stopReason"] = limit_reason
                self.state.save_job(current, status="blocked")
                return {"parentId": parent, "clicks": state.get("clicks", 0), "last": limit_reason}
            versions = dict(capture.native_versions)
            before_dom = None
            if allow_dom_progress:
                before_result, _ = self._run_ui_command(
                    reply_job["id"], "thread_list",
                    f"direct_before:{scope}:{parent}:{state.get('clicks', 0)}",
                    lambda event: native_comments.inspect_parent_ids(
                        self.ctx.session, [parent], known_parents=[parent], on_event=event))
                before_dom = next(iter(before_result.get("parents") or []), None)
            state.update(phase="click_submitting", clickedAt=time.time())
            self.state.save_thread_state(scope, parent, state)
            action, _ = self._run_ui_command(
                reply_job["id"], "reply_click",
                f"reply:{scope}:{parent}:{state.get('clicks', 0)}",
                lambda event: native_comments.drain_reply_thread(
                    self.ctx.session, parent, others,
                    wait=self.THREAD_SETTLE_SECONDS, on_event=event))
            if action.get("action") == "clicked_replies":
                state.update(phase="waiting_response",
                             clicks=state.get("clicks", 0) + 1,
                             clicksThisInvocation=state.get("clicksThisInvocation", 0) + 1,
                             absentChecks=0, last="clicked_replies")
                self.state.save_thread_state(scope, parent, state)
                before_count = self._thread_count(reply_job)
                operation = self._await_thread_response(capture, reply_job, parent, versions)
                own = capture.native_versions.get(str(parent), 0) > versions.get(str(parent), 0)
                foreign = sorted(k for k in capture.native_versions
                                 if k not in ("parents", str(parent)) and
                                 capture.native_versions.get(k, 0) > versions.get(k, 0))
                pages = self._checkpoint_capture(capture, parent_job, operation, versions)
                own = capture.native_versions.get(str(parent), 0) > versions.get(str(parent), 0)
                gained = self._thread_count(reply_job) - before_count
                if gained > 0:
                    self.state.add_thread_metric(parent, gained)
                if not own and foreign:
                    # The pressed control answered for another thread: its
                    # rows are saved under that parent by the checkpoint
                    # above; this parent's control attribution is invalid.
                    state.update(phase="misattributed", last="misattributed",
                                 misattributedTo=foreign[:3],
                                 invalidatedLocatorVersion=native_comments.LOCATOR_VERSION)
                    self.state.save_thread_state(scope, parent, state)
                    self.state.add_invocation_metric("misattributedClicks", 1)
                    return {"parentId": parent, "clicks": state.get("clicks", 0),
                            "last": "misattributed", "answeredFor": foreign[:3]}
                if not own:
                    if allow_dom_progress:
                        after_result, _ = self._run_ui_command(
                            reply_job["id"], "thread_list",
                            f"direct_after:{scope}:{parent}:{state.get('clicks', 0)}",
                            lambda event: native_comments.inspect_parent_ids(
                                self.ctx.session, [parent], known_parents=[parent], on_event=event))
                        after_dom = next(iter(after_result.get("parents") or []), None)
                        before_links = ((before_dom or {}).get("renderedLinks")
                                        if isinstance(before_dom, dict) else None)
                        after_links = ((after_dom or {}).get("renderedLinks")
                                       if isinstance(after_dom, dict) else None)
                        if (isinstance(before_links, int) and isinstance(after_links, int)
                                and after_links > before_links and after_dom.get("control")):
                            state.update(phase="visible", last="direct_dom_progress",
                                         renderedLinks=after_links,
                                         pages=state.get("pages", 0) + pages)
                            self.state.save_thread_state(scope, parent, state)
                            self.state.add_invocation_metric("directDomOnlyClicks", 1)
                            continue
                    state.update(phase="deferred", last="reply_response_missing")
                    self.state.save_thread_state(scope, parent, state)
                    current = self.state.get_job(reply_job["id"])
                    current["payload"]["stopReason"] = "reply_response_missing"
                    self.state.save_job(current, status="blocked")
                    if self.observer is not None and not self._observer_saw["replies"]:
                        # The archive may hold the response the observer
                        # never delivered; from here HAR is the source.
                        self._fall_back_to_har("observer_no_reply_response")
                    return {"parentId": parent, "clicks": state["clicks"], "last": state["last"]}
                state.update(phase="visible",
                             pages=state.get("pages", 0) + pages)
                self.state.save_thread_state(scope, parent, state)
                current = self.state.get_job(reply_job["id"])
                if (current and current["payload"].get("directRestContinuation")
                        and current["payload"].get("source") == "api"
                        and current["payload"].get("queue")):
                    state.update(phase="rest_continuation", last="direct_ui_rest_handoff")
                    self.state.save_thread_state(scope, parent, state)
                    self.state.add_invocation_metric("directRestHandoffs", 1)
                    return {"parentId": parent, "clicks": state["clicks"],
                            "last": "direct_ui_rest_handoff"}
                if current and current["payload"].get("uiExhaustionConfirmed"):
                    self._mark_thread_exhausted(scope, parent, state, current)
                    return {"parentId": parent, "clicks": state["clicks"],
                            "last": "server_exhausted"}
                continue
            if action.get("action") == "parent_not_visible":
                state.update(phase="deferred", last="parent_not_visible")
                self.state.save_thread_state(scope, parent, state)
                return {"parentId": parent, "clicks": state.get("clicks", 0),
                        "last": "parent_not_visible"}
            if action.get("action") == "ambiguous_thread_boundary":
                state.update(phase="deferred", last="ambiguous_thread_boundary",
                             boundary={"structure": action.get("structure"),
                                       "unownedControls": action.get("unownedControls")})
                self.state.save_thread_state(scope, parent, state)
                reply_job = self.state.get_job(reply_job["id"])
                if reply_job:
                    reply_job["payload"]["boundary"] = state["boundary"]
                    if (reply_job["payload"].get("directRestContinuation") and
                            reply_job["payload"].get("queue")):
                        cursors = [cursor for cursor in reply_job["payload"]["queue"]
                                   if cursor]
                        reply_job["payload"].update(
                            source="api", queue=cursors,
                            stopReason="direct_ui_rest_handoff",
                            uiReplyAttempted=True, emptyPages=0)
                        self.state.save_job(reply_job, status="pending")
                        state.update(phase="rest_continuation",
                                     last="direct_ui_rest_handoff")
                        self.state.save_thread_state(scope, parent, state)
                        self.state.add_invocation_metric("directRestHandoffs", 1)
                        return {"parentId": parent,
                                "clicks": state.get("clicks", 0),
                                "last": "direct_ui_rest_handoff"}
                    reply_job["payload"]["stopReason"] = "ambiguous_thread_boundary"
                    self.state.save_job(reply_job, status="blocked")
                return {"parentId": parent, "clicks": state.get("clicks", 0),
                        "last": "ambiguous_thread_boundary"}
            # A missing control can be a delayed render. Two more looks are
            # allowed, but only a fresh terminal response proves exhaustion.
            state["absentChecks"] = state.get("absentChecks", 0) + 1
            state.update(phase="control_absent", last=action.get("action"))
            self.state.save_thread_state(scope, parent, state)
            if state["absentChecks"] <= 2:
                operation = self._poll_capture(
                    capture, reply_job["id"],
                    f"reply_absent:{parent}:{state['absentChecks']}")
                self._checkpoint_capture(capture, parent_job, operation, versions)
                current = self.state.get_job(reply_job["id"])
                if current and current["payload"].get("uiExhaustionConfirmed"):
                    self._mark_thread_exhausted(scope, parent, state, current)
                    return {"parentId": parent, "clicks": state.get("clicks", 0),
                            "last": "server_exhausted"}
                continue
            state.update(last="ui_control_absent")
            self.state.save_thread_state(scope, parent, state)
            reply_job = self.state.get_job(reply_job["id"])
            if reply_job:
                reply_job["payload"]["stopReason"] = "ui_control_absent"
                self.state.save_job(reply_job, status="blocked")
            return {"parentId": parent, "clicks": state.get("clicks", 0),
                    "last": "ui_control_absent"}
        state.update(phase="click_cap", last="click_cap")
        self.state.save_thread_state(scope, parent, state)
        reply_job = self.state.get_job(reply_job["id"])
        if reply_job:
            reply_job["payload"]["stopReason"] = "reply_click_cap"
            self.state.save_job(reply_job, status="blocked")
        return {"parentId": parent, "clicks": state.get("clicks", 0),
                "last": "click_cap"}

    def _expand_threads_in_place(self, capture, parent_job, listing=None, known=None):
        """Drain every thread on this screen, one after another.

        Visible continuation controls come first, in list order; then the
        visible parents whose reply job is still open (a positive count,
        previews or a confirmed reply created it) even when no control is
        on screen for them right now. Each thread is taken to the end
        before the next one starts, and each click is followed by its own
        response read and durable apply before another UI command.
        """
        scope = parent_job["scope"]
        if known is None:
            known = self.state.parent_order(scope)
        if listing is None:
            listing, _ = self._run_ui_command(
                parent_job["id"], "thread_list",
                f"threads:{scope}:{parent_job['payload'].get('uiSteps', 0)}",
                lambda event: native_comments.visible_thread_parents(
                    self.ctx.session, known_parents=known, on_event=event))
        parent_controls = {
            str(p.get("parentId")): p for p in (listing.get("parents") or [])
            if isinstance(p, dict) and str(p.get("parentId") or "").isdigit()}
        parents = list(parent_controls)
        visible_rows = [str(p) for p in (listing.get("visibleParents") or []) if str(p).isdigit()]
        candidates = list(parents)
        for parent in visible_rows:
            if parent not in candidates and self._thread_unfinished(scope, parent):
                candidates.append(parent)
        # Confirmed parents: the network's order, the threads the locator
        # itself attributed, and every saved parent record.
        others = list(dict.fromkeys(list(known) + parents + [
            str(row[0]) for row in self.state.db.execute(
                "SELECT json_extract(payload,'$.id') FROM records WHERE scope=? "
                "AND json_extract(payload,'$.parentCommentId') IS NULL", (scope,)) if row[0]]))
        parent_job["payload"].update(
            uiPhase="draining_threads", discoveredThreads=candidates,
            scrollHint={key: listing.get(key) for key in
                        ("scrollTop", "scrollHeight", "clientHeight")})
        self.state.save_job(parent_job)
        threads, clicks = [], 0
        for parent in candidates:
            saved = self.state.thread_state(scope, parent) or {}
            if (saved.get("invocation") == self.state.get_meta("invocation", 1)
                    and saved.get("last") in ("reply_response_missing", "misattributed", "click_cap", "ambiguous_thread_boundary")):
                continue
            reply_job = self.state.get_job(f"replies:{scope}:{parent}")
            record = self.state.get_record(f"comment:{scope}:{parent}")
            declared = (record or {}).get("payload", {}).get("repliesCount")
            if parent in parents and declared == 0 and not saved.get("countConflict"):
                # A visible control contradicts a zero count: the UI wins,
                # the conflict is counted, the thread is still drained.
                saved["countConflict"] = True
                self.state.save_thread_state(scope, parent, saved)
                self.state.add_invocation_metric("countConflicts", 1)
            control = parent_controls.get(parent)
            allowed, evidence = self._should_open_thread(
                capture, scope, parent, saved, reply_job, control)
            if not allowed:
                metric = ("skippedCompletedThreads" if evidence ==
                          "completed_without_new_evidence" else "skippedKnownReplyViews")
                self.state.add_invocation_metric(metric, 1)
                saved.update(last=evidence, phase=("server_exhausted" if evidence ==
                             "completed_without_new_evidence" else "deferred"))
                self.state.save_thread_state(scope, parent, saved)
                continue
            completed = (saved.get("phase") == "server_exhausted" or
                         reply_job and reply_job["status"] == "done" and
                         reply_job["payload"].get("uiExhaustionConfirmed"))
            if completed:
                # Only stronger network/count evidence may reopen a terminal
                # branch. A collapsed button in a new VM is not continuation.
                reply_job = reply_job or self._ensure_reply_job(parent_job, parent)
                reply_job["payload"].update(uiExhaustionConfirmed=False,
                                             stopReason="continuation_rediscovered",
                                             continuationEvidence=evidence)
                self.state.save_job(reply_job, status="pending")
                saved.update(phase="visible", absentChecks=0,
                             continuationEvidence=evidence)
                self.state.save_thread_state(scope, parent, saved)
                self.state.add_invocation_metric("rediscoveredContinuations", 1)
            parent_job["payload"].update(activeThread=parent,
                                         uiPhase="draining_thread")
            self.state.save_job(parent_job)
            drained = self._drain_one_thread(capture, parent_job, parent, others)
            clicks += drained["clicks"] - saved.get("clicks", 0)
            threads.append(drained)
        self.state.add_invocation_metric("visibleThreads", len(parents))
        self.state.add_invocation_metric("replyClicks", clicks)
        parent_job["payload"].update(activeThread=None, uiPhase="scan_segment")
        self.state.save_job(parent_job)
        return {"visible": len(parents), "candidates": len(candidates),
                "drained": len(threads), "clicks": clicks,
                "last": listing.get("action"), "threads": threads}

    def _adopt_expanded_threads(self, capture, scope, job):
        """Give every thread expanded in place a reply job and a saved template."""
        with self.state.transaction():
            for parent, (template, connection) in list(capture.native_latest.items()):
                if (parent == "parents" or not template.get("parentId")
                        or template.get("transport") == "rest"):
                    continue
                template = native_comments.persistable(self.ctx, template)
                self.state.cache_set(f"native:{scope}:{parent}",
                                     {"template": template, "connection": connection})
            for parent in capture.reply_parents():
                # INSERT OR IGNORE underneath: an existing job is left alone.
                self._seed_replies({"pk": parent}, job)

    @staticmethod
    def _reset_ui_geometry(payload):
        payload.pop("uiProgress", None)
        payload.pop("scanProgress", None)
        payload.pop("scanComplete", None)

    def _assess_parent_movement(self, payload, action):
        """Classify settled DOM movement independently of network progress."""
        progress = payload.setdefault("uiProgress", {})
        path = action.get("path")
        if path and progress.get("path") not in (None, path):
            progress.clear()
        if path:
            progress["path"] = path
        top = action.get("settledTop", action.get("after"))
        height = action.get("settledScrollHeight", action.get("scrollHeight"))
        client = action.get("settledClientHeight", action.get("clientHeight"))
        first = action.get("settledFirstVisibleId", action.get("firstVisibleId"))
        last = action.get("settledLastVisibleId", action.get("lastVisibleId"))
        available = (action.get("action") in (
            "scrolled_comment_region", "comment_region_end", "observed_comment_region")
            and isinstance(top, (int, float)) and isinstance(height, (int, float))
            and isinstance(client, (int, float)))
        signature = f"{first}:{last}" if first or last else None
        seen = list(progress.get("seenViewports") or [])
        max_top = progress.get("maxSettledTop", -1)
        last_top = progress.get("lastSettledTop")
        new_viewport = bool(signature and signature not in seen)
        moved = bool(available and (
            top > max_top + 2 or
            (new_viewport and (last_top is None or top >= last_top - 2))))
        if moved:
            progress["maxSettledTop"] = max(max_top, top)
            progress["noMovementStreak"] = 0
        else:
            progress["noMovementStreak"] = progress.get("noMovementStreak", 0) + 1
        if signature and signature not in seen:
            seen.append(signature)
            progress["seenViewports"] = seen[-64:]
        if available:
            progress.update(lastSettledTop=top, lastScrollHeight=height,
                            lastClientHeight=client, firstVisibleId=first,
                            lastVisibleId=last)
        immediate = action.get("immediate", action.get("after"))
        before = action.get("before")
        rolled_back = bool(isinstance(before, (int, float)) and
                           isinstance(immediate, (int, float)) and
                           immediate > before + 2 and top < immediate - 2)
        at_end = bool(available and (
            action.get("settledAtEnd") is True or
            top >= max(0, height - client) - 2))
        return {
            "regionAvailable": available, "moved": moved,
            "newViewport": new_viewport, "rolledBack": rolled_back,
            "atEnd": at_end, "settledTop": top,
            "scrollHeight": height, "clientHeight": client,
            "firstVisibleId": first, "lastVisibleId": last,
        }

    def _parent_ui_step(self, capture, job, fingerprint, *, wait):
        """Perform one scroll and bounded observation-only stall recovery."""
        payload = job["payload"]
        previous_version = capture.native_versions.get("parents", 0)
        versions = dict(capture.native_versions)
        action, _ = self._run_ui_command(
            job["id"], "parent_scroll", fingerprint,
            lambda event: native_comments.advance_ui(
                self.ctx.session, wait=wait, on_event=event))
        payload["uiSteps"] = payload.get("uiSteps", 0) + 1
        self.state.add_invocation_metric("parentScrollSteps", 1)
        operation = self._poll_capture(capture, job["id"], "parent_scroll")
        self._checkpoint_capture(capture, job, operation, versions)
        accepted = max(0, capture.native_versions.get("parents", 0) - previous_version)
        assessment = self._assess_parent_movement(payload, action)
        rolled_back = assessment["rolledBack"]
        checks = []
        while (not assessment["moved"] and not accepted and
               len(checks) < self.UI_SETTLE_CHECKS):
            self._budget()
            self.state.add_invocation_metric("uiSettleChecks", 1)
            check_versions = dict(capture.native_versions)
            check, _ = self._run_ui_command(
                job["id"], "parent_settle",
                f"{fingerprint}:settle:{len(checks) + 1}",
                lambda event: native_comments.observe_ui(
                    self.ctx.session, wait=5.0, on_event=event))
            check_operation = self._poll_capture(
                capture, job["id"], "parent_settle")
            self._checkpoint_capture(
                capture, job, check_operation, check_versions)
            operation = check_operation
            newly_accepted = max(
                0, capture.native_versions.get("parents", 0) -
                previous_version - accepted)
            if newly_accepted:
                accepted += newly_accepted
            check_assessment = self._assess_parent_movement(payload, check)
            rolled_back = rolled_back or check_assessment["rolledBack"]
            checks.append({**check_assessment, "action": check.get("action")})
            if check_assessment["moved"]:
                assessment = check_assessment
                for key, value in check.items():
                    if key.startswith("settled"):
                        action[key] = value
            elif check_assessment["regionAvailable"]:
                assessment = check_assessment
            if assessment["moved"] or accepted:
                break
        assessment["rolledBack"] = rolled_back
        if assessment["moved"] or accepted:
            decision = "continue"
        elif not assessment["regionAvailable"]:
            decision = "ui_region_unavailable"
        elif assessment["atEnd"]:
            decision = "ui_pagination_unresponsive"
        else:
            decision = "ui_scroll_stalled"
        diagnostic = {
            "uiStep": payload["uiSteps"], "action": action.get("action"),
            **assessment, "acceptedParentPages": accepted,
            "decision": decision, "settleChecks": checks,
        }
        history = list(payload.get("uiStepDiagnostics") or [])
        history.append(diagnostic)
        payload["uiStepDiagnostics"] = history[-100:]
        payload["uiLastDecision"] = decision
        action["progress"] = diagnostic
        self.state.save_job(job)
        return action, operation, previous_version, decision

    def _ensure_reply_job(self, parent_job, parent, capture=None):
        job_id = f"replies:{parent_job['scope']}:{parent}"
        job = self.state.get_job(job_id)
        if job:
            return job
        latest = capture.native_latest.get(str(parent)) if capture else None
        if latest and latest[0].get("transport") != "rest":
            template = native_comments.persistable(self.ctx, latest[0])
            self.state.cache_set(f"native:{parent_job['scope']}:{parent}",
                                 {"template": template, "connection": latest[1]})
            self._seed_replies({"pk": str(parent)}, parent_job)
            job = self.state.get_job(job_id)
            if job:
                return job
        record = self.state.get_record(f"comment:{parent_job['scope']}:{parent}")
        expected = (record or {}).get("payload", {}).get("repliesCount")
        self.state.enqueue(job_id, "replies", parent_job["scope"], {
            "code": parent_job["payload"]["code"],
            "inputUrl": parent_job["payload"]["inputUrl"],
            "parentId": str(parent), "expected": expected, "queue": [{}],
            "pages": 0, "dataPages": 0, "seenCursors": [], "emptyPages": 0,
            "source": "native_ui", "uiReplyAttempted": True})
        return self.state.get_job(job_id)

    def _apply_observed_page(self, page):
        job = self.state.get_job(page["job_id"])
        if not job:
            self.state.mark_observed_page_applied(page["id"])
            return 0
        replies = job["kind"] == "replies"
        rows = page["payload"].get("child_comments" if replies else "comments") or []
        before = self.state.count(job["scope"])
        if self.state.get_meta("recoveredCommandsThisInvocation", 0):
            self.state.add_invocation_metric("pagesAfterRecovery", 1)
        with self.state.transaction():
            for raw in rows:
                if not isinstance(raw, dict):
                    continue
                if not self._save_comment(raw, job, job["payload"].get("parentId") if replies else None, page["source"]):
                    if self.state.count(job["scope"]) >= self.config.results_limit:
                        self.limited_scopes.add(job["scope"])
                        return self.state.count(job["scope"]) - before
                    continue
                if not replies:
                    self._seed_replies(raw, job)
            observation = page["payload"].get("_uiObservation") or {}
            if not replies and "_parentObservation" in page["payload"]:
                parent_observation = page["payload"]["_parentObservation"]
                n = int(parent_observation.get("responsePages") or 0)
                job["payload"]["pages"] = job["payload"].get("pages", 0) + n
                job["payload"]["dataPages"] = job["payload"].get("dataPages", 0) + n
                if n:
                    job["payload"]["parentNetworkTerminal"] = parent_observation.get("hasNextPage") is False
                    job["payload"]["uiEndCursor"] = parent_observation.get("endCursor")
                    sizes = list(job["payload"].get("parentPageSizes") or [])
                    if rows:
                        sizes.extend([len(rows)] * n)
                    job["payload"]["parentPageSizes"] = sizes[-100:]
                    self.state.add_invocation_metric("acceptedParentPages", n)
                if job["payload"].get("uiRecovering"):
                    delivered = list(job["payload"].get("recoveryObservedParents") or [])
                    known_delivered = set(delivered)
                    for raw in rows:
                        parent_id = identity(raw) if isinstance(raw, dict) else None
                        if parent_id is not None and str(parent_id) not in known_delivered:
                            delivered.append(str(parent_id))
                            known_delivered.add(str(parent_id))
                    job["payload"]["recoveryObservedParents"] = delivered[-10000:]
                dom_queue = list(job["payload"].get("parentDomCheckQueue") or [])
                queued = set(dom_queue)
                for raw in rows:
                    parent_id = identity(raw) if isinstance(raw, dict) else None
                    if parent_id is not None and str(parent_id) not in queued:
                        dom_queue.append(str(parent_id))
                        queued.add(str(parent_id))
                job["payload"]["parentDomCheckQueue"] = dom_queue[-10000:]
                self.state.save_job(job, status=job["status"])
            if replies:
                job["payload"]["uiLastObservation"] = observation
                job["payload"]["uiExhaustionConfirmed"] = bool(
                    observation.get("exhaustionConfirmed"))
                response_pages = max(0, int(observation.get("responsePages") or 0))
                job["payload"]["dataPages"] = (
                    job["payload"].get("dataPages", 0) + response_pages)
                following, cursor_issues = page_cursors(page["payload"], True)
                for cursor in following:
                    if (cursor not in job["payload"].get("seenCursors", []) and
                            cursor not in job["payload"].get("queue", [])):
                        job["payload"].setdefault("queue", []).append(cursor)
                if following:
                    if any(key in page["payload"] for key in (
                            "next_min_child_cursor", "next_max_child_cursor")):
                        job["payload"]["source"] = "api"
                        job["payload"]["queue"] = [cursor for cursor in
                            job["payload"].get("queue", []) if cursor]
                    job["payload"]["directRestContinuation"] = True
                    job["payload"]["directRestCursorIssues"] = cursor_issues
                if (observation.get("freshResponse") and
                        observation.get("observedCursor") is not None):
                    self.state.set_meta("nativeRepliesValidated", True)
                expected = job["payload"].get("expected")
                count = self._thread_count(job)
                if observation.get("exhaustionConfirmed"):
                    job['payload']['serverExhaustionConfirmed'] = True
                    if isinstance(expected, int) and count < expected:
                        job["payload"]["stopReason"] = "reported_count_gap"
                        status = "blocked"
                    else:
                        job["payload"].pop("stopReason", None)
                        status = "done"
                else:
                    job["payload"].pop("stopReason", None)
                    status = "pending"
                self.state.save_job(job, status=status)
            self.state.mark_observed_page_applied(page["id"])
        return self.state.count(job["scope"]) - before

    def register_media(self, code, input_url):
        scope = str(shortcode_to_media_id(code))
        self.state.enqueue(f"parents:{scope}", "parents", scope,
                           {"code": code, "inputUrl": input_url, "queue": [{}], "seenCursors": [],
                            "pages": 0, "dataPages": 0, "emptyPages": 0,
                            "preloaded": False, "source": "rest"})

    def register(self, targets):
        from .scrapers import scraper_for
        from .scrapers.posts import PostsScraper
        for target in targets:
            self._budget()
            target = resolve_user_id(target, self.api)
            self.scrape.remember_user_id(target.key, (target.extra or {}).get("user_id"))
            saved = self.state.db.execute("SELECT status FROM targets WHERE id=?", (target.url,)).fetchone()
            if saved and saved[0] == "done":
                continue
            if self.config.results_type.value == "comments":
                if not self.ctx.authenticated:
                    raise LoginRequiredError("comments")
                if target.type.is_media:
                    with self.state.transaction():
                        self.register_media(target.key, target.input_url)
                        self.state.db.execute("INSERT OR REPLACE INTO targets VALUES(?,?)", (target.url, "done"))
                elif target.type.is_feed:
                    config = replace(self.config, results_limit=self.config.comment_post_limit or self.config.results_limit)
                    discovery = PostsScraper(replace(self.scrape, config=config))
                    discovered = 0
                    for post in discovery.run(target):
                        self._budget()
                        if post.get("shortCode"):
                            discovered += 1
                            with self.state.transaction():
                                self.register_media(post["shortCode"], target.input_url)
                        self.storage.dataset.maybe_flush()
                    self.state.db.execute("INSERT OR REPLACE INTO targets VALUES(?,?)",
                                          (target.url, "limit" if discovered >= config.results_limit else "done"))
                else:
                    raise InstagramError(f"Unsupported comments target: {target.type.value}")
            else:
                scraper = scraper_for(self.config.results_type, self.scrape)
                emitted = 0
                for record in scraper.run(target):
                    self._budget()
                    with self.state.transaction():
                        kind = "post" if record.get("shortCode") else self.config.results_type.value
                        if kind == "post":
                            for field in map_post({}):
                                record.setdefault(field, None)
                        observed = record.pop("_observed_at", None)
                        key = self.state.upsert(record, scope=target.url, kind=kind, observed=observed,
                                                source=record.get("dataSource", "api"))
                        if kind == "post":
                            self._enqueue_media(key, target.url, record)
                    emitted += 1
                    self.storage.dataset.maybe_flush()
                self.state.db.execute("INSERT OR REPLACE INTO targets VALUES(?,?)",
                                      (target.url, "limit" if target.type.is_feed and emitted >= self.config.results_limit else "done"))

    def _enqueue_media(self, key, scope, record):
        if record.get("shortCode"):
            self.state.enqueue(f"media:{key}", "enrich", scope,
                               {"type": "media", "recordKey": key, "code": record["shortCode"]})

    def _owner_rows(self, owner_id, username):
        return self.state.db.execute("SELECT key FROM records WHERE json_extract(payload,'$.owner.id')=? OR json_extract(payload,'$.ownerUsername')=? OR json_extract(payload,'$.ownerId')=?",
                                     (owner_id, username, owner_id))

    def _apply_owner(self, key, raw):
        row = self.state.get_record(key)
        if not raw.get("_unavailable"):
            owner = {k: raw[k] for k in OWNER_FIELDS if k in raw}
            owner["id"] = str(raw.get("id") or raw.get("pk") or "")
            patch = {"id": row["payload"]["id"], "ownerUsername": owner.get("username")}
            if row["kind"] == "comment" or self.config.expand_owners:
                patch.update(owner=owner, ownerProfilePicUrl=owner.get("profile_pic_url"))
            if row["kind"] == "post":
                patch.update(ownerFullName=owner.get("full_name"), ownerId=owner["id"])
            source = "profile_by_id" if raw.get("_profile_source") == "user_by_id" else "profile_info"
            self.state.upsert(patch, scope=row["scope"], kind=row["kind"], source=source,
                              observed=raw.get("_observed_at"), known={f"owner.{k}" for k in owner})
        if raw.get("_profile_source") != "user_by_id":
            missing = [f"owner.{k}" for k in OWNER_FIELDS] if row["kind"] == "comment" or self.config.expand_owners else []
            missing += [k for k in row["payload"] if k.startswith("owner") and k != "owner"]
            self.state.mark_unavailable(key, missing, "profile_not_found" if raw.get("_unavailable") else "profile_info")

    def _queue_owner(self, key, record):
        if not self.config.expand_owners:
            return
        owner_id = (record.get("owner") or {}).get("id") or record.get("ownerId") or record.get("ownerUsername")
        username = record.get("ownerUsername")
        if owner_id and username:
            cache_key = f"owner:{owner_id}"
            cached = self.state.cache_get(cache_key)
            if cached:
                self._apply_owner(key, cached)
            else:
                self.state.enqueue(cache_key, "enrich", "owners",
                                   {"type": "owner", "ownerId": str(owner_id), "username": username})

    def _save_comment(self, raw, job, parent=None, source="api"):
        payload, scope = job["payload"], job["scope"]
        raw = node(raw)
        cid = raw.get("pk") or raw.get("id")
        if cid is None:
            return False
        if self.state.count(scope) >= self.config.results_limit and not self.state.exists("comment", scope, cid):
            return False
        if parent:
            raw = {**raw, "_parent_comment_id": parent}
        record = map_comment(raw, post_url=post_url(payload["code"]), input_url=payload["inputUrl"])
        if not parent:
            # The network's order of parents is the traversal's map -- also
            # for a parent outside the date window, whose thread may hold
            # replies inside it.
            self.state.append_parent_order(scope, [str(cid)])
        if not within_dates(self.config, record):
            # ``onlyPostsNewerThan`` / ``untilDate`` apply to a comment's own
            # timestamp. Ranked lists are not monotonic in time, so skipping
            # one record does not stop their traversal. Chronological parents
            # have a separate page-level date-floor stop.
            self.state.add_invocation_metric("dateFilteredComments", 1)
            return True
        record["dataSource"] = source
        for field in COMMENT_FIELDS:
            record.setdefault(field, None)
        key = self.state.upsert(record, scope=scope, kind="comment", source=source, observed=raw.get("_observed_at"),
                                known=known_fields(raw, COMMENT_FIELDS))
        self._queue_owner(key, record)
        self.state.mark_unavailable(key, [field for field in COMMENT_FIELDS if field not in known_fields(raw, COMMENT_FIELDS)], source)
        return True

    def _seed_replies(self, raw, job):
        raw = node(raw)
        parent = raw.get("pk") or raw.get("id")
        previews = raw.get("preview_child_comments") or (raw.get("edge_threaded_comments") or {}).get("edges") or []
        expected = raw.get("child_comment_count")
        if expected is None:
            expected = (raw.get("edge_threaded_comments") or {}).get("count")
        native = self.state.cache_get(f"native:{job['scope']}:{parent}") if parent is not None else None
        first = self.state.cache_get(f"reply-first:{job['scope']}:{parent}") if parent is not None else None
        if not self.config.include_nested_comments or parent is None or not (previews or expected or native or first):
            return
        cap = self.config.max_replies_per_comment
        unique = {}
        for child in previews:
            if not isinstance(child, dict):
                continue
            unique[identity(child)] = child
        if cap == 0:
            return
        payload = {"code": job["payload"]["code"], "inputUrl": job["payload"]["inputUrl"],
                   "parentId": str(parent), "expected": expected, "queue": [{}], "pages": 0,
                   "dataPages": 0, "seenCursors": [], "emptyPages": 0,
                   "source": "rest", "seeds": list(unique.values())}
        if native:
            payload.update(source="native", template=native["template"])
            if self.observer is not None or native["template"].get("friendlyName") in native_comments.UI_REPLY_QUERIES:
                payload.update(source="native_ui", uiReplyAttempted=True)
                paid = native_comments.page(native["connection"], replies=True)["child_comments"]
                for raw in paid:
                    unique[identity(raw)] = raw
                payload["seeds"] = list(unique.values())
            elif native["template"].get("observedCursor") is None:
                payload["first"] = native_comments.page(native["connection"], replies=True)
        else:
            if first:
                payload["first"] = first
        self.state.enqueue(f"replies:{job['scope']}:{parent}", "replies", job["scope"], payload)

    def _preload(self, job):
        payload, scope = job["payload"], job["scope"]
        payload["pageLoadAttempts"] = payload.get("pageLoadAttempts", 0) + 1
        if payload["pageLoadAttempts"] > 1:
            self.state.set_meta("repeatedPageLoads", self.state.get_meta("repeatedPageLoads", 0) + 1)
        if not (self.config.is_newest_comments and self._can_reuse_post_document()):
            self.state.set_meta("pageLoads", self.state.get_meta("pageLoads", 0) + 1)
        self.state.save_job(job)
        expected = post_url(payload["code"])
        if self.config.is_newest_comments:
            # The chronological model (`isNewestComments`): the parent list is
            # REST `sort_order=recent`, its head cursor read in page context.
            # Measured 18 September on a 4.4k-comment post: 235 pages, 3387
            # parents, the real terminal, no UI, no observer, no HAR
            # (RECENT-ORDER-DEEP-2026-09-18.md). With reuse enabled, the
            # first post document also serves later media IDs; the ranked
            # traversal is not needed.
            self._open_post_document(payload)
            payload.update(source="rest", chronological=True, preloaded=True,
                           queue=payload.get("queue") or [{}])
            self.state.save_job(job)
            return
        self._reset_ui_geometry(payload)
        self.state.reset_parent_dom_observations(scope)
        for key in ("gapAuditQueue", "gapAuditSummary", "gapAuditStartedAt",
                    "gapAuditLast", "gapAuditSteps"):
            payload.pop(key, None)
        if payload.get("lastScannedId"):
            # A new document (resume, or a re-opened post): the rows examined
            # so far are not loaded yet. Restore the examination by id.
            self._begin_anchor_recovery(payload, scope)
            self.state.save_job(job)
        landed = self.ctx.goto(expected, wait=3, refresh_tokens=True)
        if not self.ctx.authenticated:
            raise FatalError("Post page rejected authenticated access")
        if landed:
            try:
                actual = parse_target(landed)
            except (ValueError, InstagramError, UnsupportedUrlError):
                actual = None
            if actual is None or not actual.type.is_media or actual.key != payload["code"]:
                raise NotFoundError(
                    f"Post {payload['code']} redirected away from its permalink"
                )
        capture, ui_actions = self._capture_native(scope, job)
        self.native_captures[scope] = capture
        with self.state.transaction():
            payload["nativeObservation"] = {"eligibleConnections": len(capture.native),
                                            "responsesExamined": capture.responses_seen,
                                            "graphqlResponses": capture.native_graphql_responses,
                                            "decodedGraphqlBodies": capture.native_diagnostics.get("decodedBodies", 0),
                                            "acceptedQueries": capture.native_diagnostics.get("acceptedQueries", 0),
                                            "rejectionReasons": capture.native_diagnostics.get("rejectionReasons", {}),
                                            "rejectionSamples": capture.native_diagnostics.get("rejectionSamples", []),
                                            "uiActions": ui_actions,
                                            "status": "observed" if capture.native else "no_supported_observed_query"}
            for parent, (template, connection) in list(capture.native.items()):
                template = native_comments.persistable(self.ctx, template)
                capture.native[parent] = (template, connection)
                self.state.cache_set(f"native:{scope}:{parent}", {"template": template, "connection": connection})
            for parent, body in capture.reply_pages.items():
                self.state.cache_set(f"reply-first:{scope}:{parent}", body)
            if "parents" in capture.native:
                template, connection = capture.native["parents"]
                # The filtered export is a candidate replacement for the
                # observer as the *source*; the traversal it feeds is the
                # observer's UI traversal, not the old template replay.
                if (self.observer is not None or self._har_resource_types is not None
                        or template.get("friendlyName") == "PolarisPostCommentsPaginationQuery"):
                    payload.update(source="native_ui", template=template, uiStalls=0)
                    latest_parent = capture.native_latest.get("parents")
                    if (template.get("observedCursor") is not None or
                            latest_parent and latest_parent[0].get("observedCursor") is not None):
                        payload["template"]["validation"] = "pagination_confirmed"
                        self.state.set_meta("nativeParentsValidated", True)
                    observed = latest_parent[0] if latest_parent else template
                    payload["uiExhaustionConfirmed"] = observed.get("observedHasNext") is False
                    payload["uiEndCursor"] = observed.get("observedEndCursor")
                    payload["parentNetworkTerminal"] = payload["uiExhaustionConfirmed"]
                    payload["uiExhaustionConfirmed"] = False
                    payload["queue"] = [{"ui_round": 1}]
                else:
                    payload.update(source="native", template=template)
                    if template.get("observedCursor") is None:
                        payload["first"] = native_comments.page(connection)
                    elif template.get("observedHasNext") and template.get("observedEndCursor"):
                        payload["queue"] = [{"max_id": str(template["observedEndCursor"])}]
                    elif template.get("observedHasNext") is False:
                        payload["queue"] = []
            elif capture.first_comment_page is not None:
                payload["first"] = capture.first_comment_page
            payload["seeds"] = list(capture.comments.values())
            if payload.get("source") == "native_ui":
                capture.comments.clear()
            payload["preloaded"] = True
            self.state.save_job(job)

    def _can_reuse_post_document(self):
        if not (self.config.is_newest_comments and self.config.reuse_post_document
                and self.ctx.authenticated):
            return False
        try:
            return parse_target(self.ctx.current_page).type.is_media
        except (ValueError, InstagramError, UnsupportedUrlError):
            return False

    def _open_post_document(self, payload):
        """Keep one authenticated document for chronological API reads when enabled."""
        expected = post_url(payload["code"])
        if self.ctx.current_page.rstrip("/") == expected.rstrip("/"):
            return
        if self._can_reuse_post_document():
            if payload["code"] not in self._reused_post_codes:
                self._reused_post_codes.add(payload["code"])
                self.state.add_invocation_metric("postNavigationsAvoided", 1)
            return
        landed = self.ctx.goto(expected, wait=3, refresh_tokens=True)
        if not self.ctx.authenticated:
            raise FatalError("Post page rejected authenticated access")
        if landed:
            try:
                actual = parse_target(landed)
            except (ValueError, InstagramError, UnsupportedUrlError):
                actual = None
            if actual is None or not actual.type.is_media or actual.key != payload["code"]:
                raise NotFoundError(f"Post {payload['code']} redirected away from its permalink")

    def _past_date_floor(self, rows):
        """True when every dated row on a chronological page is older than
        `onlyPostsNewerThan`: newer pages cannot follow, the list is done."""
        floor = self.config.only_posts_newer_than
        if not floor:
            return False
        stamps = []
        for raw in rows:
            raw = node(raw) if isinstance(raw, dict) else {}
            created = raw.get("created_at") or raw.get("created_at_utc")
            if created is not None:
                try:
                    stamps.append(float(created))
                except (TypeError, ValueError):
                    pass
        return bool(stamps) and max(stamps) < floor.timestamp()

    def _thread_count(self, job):
        parent = job["payload"].get("parentId")
        return self.state.db.execute("SELECT count(*) FROM records WHERE kind='comment' AND scope=? AND json_extract(payload,'$.parentCommentId')=?",
                                     (job["scope"], parent)).fetchone()[0]

    def _page(self, job):
        payload, scope = job["payload"], job["scope"]
        replies = job["kind"] == "replies"
        saved_page = self.state.pending_page(job["id"])
        if not saved_page and self.state.count(scope) >= self.config.results_limit:
            payload["stopReason"] = "results_limit"
            self.state.save_job(job)
            self.limited_scopes.add(scope)
            return
        if replies and self.config.max_replies_per_comment is not None and self._thread_count(job) >= self.config.max_replies_per_comment:
            payload["stopReason"] = "reply_limit"
            self.state.save_job(job, status="blocked")
            return
        if not replies and not payload.get("preloaded"):
            self._preload(job)
        data_pages = payload.get("dataPages", payload.get("pages", 0))
        if (not saved_page and data_pages >= self.config.max_comment_pages
                and not (payload.get("source") == "native_ui" and not replies and
                         payload.get("uiPhase") != "load_parents")):
            payload["stopReason"] = "page_limit"
            self.state.save_job(job, status="blocked")
            return
        if not payload["queue"] and not payload.get("seeds") and not self.state.pending_page(job["id"]) and not payload.get("buffer"):
            self.state.save_job(job, status="done")
            return
        params = payload["queue"][0] if payload["queue"] else {}
        is_seed = bool(payload.get("seeds"))
        buffered = payload.get("buffer")
        if saved_page:
            body, source = saved_page["payload"], saved_page["source"]
        elif is_seed:
            body = {"child_comments" if replies else "comments": payload["seeds"]}
            source = "network"
        elif buffered:
            body, source = buffered["body"], buffered["source"]
        elif "first" in payload and not params:
            body = payload["first"]
            source = "network"
        elif payload["source"] == "direct_ui":
            self._direct_parent_thread(job)
            return
        elif payload["source"] == "native_ui":
            expected = post_url(payload["code"])
            if self.ctx.current_page.rstrip("/") != expected.rstrip("/"):
                self._reset_ui_geometry(payload)
                self.state.reset_parent_dom_observations(scope)
                for key in ("gapAuditQueue", "gapAuditSummary", "gapAuditStartedAt",
                            "gapAuditLast", "gapAuditSteps"):
                    payload.pop(key, None)
                if payload.get("lastScannedId"):
                    self._begin_anchor_recovery(payload, scope)
                    self.state.save_job(job)
                landed = self.ctx.goto(expected, wait=3, refresh_tokens=True)
                if not self.ctx.authenticated:
                    raise FatalError("Post page rejected authenticated access")
                if landed and parse_target(landed).key != payload["code"]:
                    raise NotFoundError("Native UI post redirected away from its permalink")
                # Navigation resets DOM pagination even in the same VM.
                self.native_captures.pop(scope, None)
            capture = self.native_captures.get(scope)
            if capture is None:
                capture = NetworkCapture(
                    media_id=scope, newest_first=self.config.is_newest_comments,
                    stop_on_rejection=True,
                    rejection_since=getattr(self.ctx, "authenticated_since", None),
                    fingerprint_store=self.state, defer_claims=True)
                if self.observer is not None:
                    self._install_observer(job["id"])
                elif self._witness is not None:
                    self._install_witness(job["id"])
                operation = self._har_poll(capture, job["id"], "restore")
                self._checkpoint_capture(capture, job, operation, {})
                if self.observer is not None or self._witness is not None:
                    self._initial_snapshot_ids |= set(capture.seen_comment_ids)
                self.native_captures[scope] = capture
            cached = capture.reply_rows(payload["parentId"]) if replies else list(capture.comments.values())
            if capture.disabled and not cached:
                payload["stopReason"] = "ui_capture_unavailable"
                self.state.save_job(job, status="blocked")
                return
            capture_key = payload["parentId"] if replies else "parents"
            if replies:
                # Reply UI is consumed only while the common parent pass has
                # the thread on screen. Never scroll the whole list once per
                # invisible parent.
                payload["stopReason"] = "awaiting_parent_pass"
                self.state.save_job(job, status="blocked")
                return
            prior_cursor = payload.get("uiEndCursor")
            action, operation, previous_version, ui_decision, threads = self._ui_phase_step(
                capture, job)
            action["threads"] = threads
            if operation is None:
                operation = self.state.begin_operation(
                    job["id"], "ui_phase", hashlib.sha256(
                        f"phase:{scope}:{payload.get('uiSteps', 0)}:{action.get('action')}".encode()).hexdigest(), 1)
                self.state.update_operation(operation, "decoded", phase="ui_phase", last_status="done")
            self._adopt_expanded_threads(capture, scope, job)
            self._maybe_verify_observer(capture, job)
            latest = capture.native_latest.get("parents")
            has_next = latest[0].get("observedHasNext") if latest else None
            fresh = capture.native_versions.get("parents", 0) > previous_version
            response_pages = max(
                0, capture.native_versions.get("parents", 0) - previous_version)
            rows = (native_comments.page(latest[1]).get("comments", [])
                    if latest and fresh else [])
            end_cursor = latest[0].get("observedEndCursor") if latest else None
            if latest:
                payload["template"] = native_comments.persistable(self.ctx, latest[0])
            exhausted = bool(payload.get("parentNetworkTerminal") and payload.get("scanComplete"))
            can_continue = not exhausted
            body = {"comments": rows, "has_more_comments": can_continue,
                    "next_max_id": (f"ui:{payload.get('uiSteps', 0)}:{payload.get('scanSteps', 0)}:{payload.get('uiRounds', 0)}:{payload.get('gapAuditSteps', 0)}"
                                    if can_continue else None),
                    "_uiObservation": {"action": action, "freshResponse": fresh,
                        "priorEndCursor": prior_cursor,
                        "responsePages": response_pages,
                        "phase": action.get("phase"), "nextPhase": payload.get("uiPhase"),
                        "hasNextPage": has_next, "endCursor": end_cursor,
                        "template": payload.get("template", {}),
                        "exhaustionConfirmed": exhausted,
                        "captureDisabled": capture.disabled,
                        "stopReason": (ui_decision if ui_decision in self.UI_STOPS else None)}}
            self.state.save_pending_page(job["id"], operation, "native_ui", body)
            saved_page = self.state.pending_page(job["id"])
            source = "native_ui"
        elif payload["source"] == "native":
            if self.ctx.current_page.rstrip("/") != post_url(payload["code"]).rstrip("/"):
                expected = post_url(payload["code"])
                landed = self.ctx.goto(expected, wait=3, refresh_tokens=True)
                if not self.ctx.authenticated:
                    raise FatalError("Post page rejected authenticated access")
                if landed:
                    try:
                        actual = parse_target(landed)
                    except (ValueError, InstagramError, UnsupportedUrlError):
                        actual = None
                    if actual is None or not actual.type.is_media or actual.key != payload["code"]:
                        raise NotFoundError(
                            f"Post {payload['code']} redirected away from its permalink"
                        )
            if not native_comments.runtime_available(self.ctx, payload["template"]):
                if replies:
                    raise InstagramError("Native reply query must be re-observed before resume")
                capture, actions = self._capture_native(scope, job)
                observed = capture.native.get("parents")
                if not observed:
                    raise InstagramError("Native parent query could not be re-observed after resume")
                template, connection = observed
                template = native_comments.persistable(self.ctx, template)
                payload["template"] = template
                merged = {str(identity(raw)): raw for raw in payload.get("seeds", [])
                          if isinstance(raw, dict) and identity(raw)}
                merged.update({str(identity(raw)): raw for raw in capture.comments.values()
                               if isinstance(raw, dict) and identity(raw)})
                payload["seeds"] = list(merged.values())
                if template.get("observedHasNext") and template.get("observedEndCursor"):
                    payload["queue"] = [{"max_id": str(template["observedEndCursor"])}]
                elif template.get("observedHasNext") is False:
                    payload["queue"] = []
                payload["nativeResumeObservation"] = {"uiActions": actions,
                                                       "responsesExamined": capture.responses_seen,
                                                       "graphqlResponses": capture.native_graphql_responses,
                                                       "rejectionReasons": capture.native_diagnostics.get("rejectionReasons", {}),
                                                       "rejectionSamples": capture.native_diagnostics.get("rejectionSamples", [])}
                self.state.save_job(job)
                return
            body = native_comments.page(native_comments.fetch(self.ctx, payload["template"], params.get("max_id")), replies=replies)
            source = "native"
        else:
            url = ep.child_comments(scope, payload["parentId"], **params) if replies else ep.media_comments(
                scope, sort_order="recent" if self.config.is_newest_comments else None, **params)
            if not replies and payload.get("chronological"):
                self._open_post_document(payload)
            # Head cursors page on from the document's fetch context. A
            # bounded 28 September probe also obtained the first five recent
            # parent pages from that same context (70 distinct IDs); mixing a
            # fetch_json first page with page-context continuation stalled on
            # two posts. Keep each chronological parent chain in one context.
            # Reply first pages and tail reads retain their existing path.
            transport = "page" if params.get("min_id") or (
                not replies and payload.get("chronological")) else "fetch_json"
            body = self.ctx.api_get(url, what="comment page", transport=transport)
            source = "api"
            if transport == "page":
                self.state.add_invocation_metric("pageFetchReads", 1)
            if not replies and payload.get("chronological"):
                self.state.add_invocation_metric("chronologicalPages", 1)
            saved_page = self.state.pending_page(job["id"])
        rows = body.get("child_comments" if replies else "comments")
        if not isinstance(rows, list):
            raise InstagramError("Comment response omitted its list")
        with self.state.transaction():
            before = self.state.count(scope)
            remaining = []
            for index, raw in enumerate(rows):
                if not isinstance(raw, dict):
                    continue
                if node(raw).get("pk") is None and node(raw).get("id") is None:
                    continue
                if replies and self.config.max_replies_per_comment is not None and self._thread_count(job) >= self.config.max_replies_per_comment:
                    remaining = rows[index:]
                    break
                if not self._save_comment(raw, job, payload.get("parentId"), source):
                    remaining = rows[index:]
                    break
                if not replies:
                    self._seed_replies(raw, job)
            gained = self.state.count(scope) - before
            if remaining:
                if is_seed:
                    payload["seeds"] = remaining
                else:
                    payload["buffer"] = {"body": {**body, "child_comments" if replies else "comments": remaining}, "source": source}
                self.state.save_job(job)
                if self.state.count(scope) >= self.config.results_limit:
                    payload["stopReason"] = "results_limit"
                    self.state.save_job(job)
                    self.limited_scopes.add(scope)
                else:
                    payload["stopReason"] = "reply_limit"
                    self.state.save_job(job, status="blocked")
                if saved_page:
                    self.state.mark_pending_page_applied(job["id"])
                return
            if is_seed:
                payload.pop("seeds", None)
                # A preview count is not an exhausted pagination response.
                self.state.save_job(job)
                if saved_page:
                    self.state.mark_pending_page_applied(job["id"])
                return
            payload.pop("buffer", None)
            if payload["queue"]:
                payload["queue"].pop(0)
            payload.pop("first", None)
            payload["seenCursors"].append(params)
            page_ids = sorted(str(identity(raw)) for raw in rows if isinstance(raw, dict) and identity(raw))
            empty = not page_ids
            advances = bool(page_ids) and page_ids != payload.get("lastPageIds")
            if source == "native_ui":
                observation = body.get("_uiObservation") or {}
                cursor = observation.get("endCursor")
                advances = (gained > 0 or bool(observation.get("freshResponse") and
                            cursor and cursor != observation.get("priorEndCursor", payload.get("uiEndCursor"))) or bool(
                            observation.get("seekCursor") and observation["seekCursor"] != payload.get("uiSeekCursor")))
                payload["template"] = observation.get("template", payload.get("template", {}))
                payload["uiSeekCursor"] = observation.get("seekCursor") or payload.get("uiSeekCursor")
                payload["uiLastObservation"] = observation
                payload["uiEndCursor"] = cursor or payload.get("uiEndCursor")
                payload["uiExhaustionConfirmed"] = bool(observation.get("exhaustionConfirmed"))
                payload["uiRounds"] = payload.get("uiRounds", 0) + 1
                # Response pages and cursors were committed by _checkpoint_capture.
            else:
                payload["dataPages"] = payload.get("dataPages", 0) + 1
                payload["pages"] += 1
            payload["emptyPages"] = 0 if advances else payload.get("emptyPages", 0) + 1
            payload["lastPageIds"] = page_ids
            following, issues = page_cursors(body, replies)
            if (not replies and payload.get("chronological") and following
                    and self._past_date_floor(rows)):
                # Newest first: once a whole page predates the floor, so does
                # everything after it. The list is complete for the window.
                following = []
                payload["stopReason"] = "date_floor"
            if source == "native_ui" and following and (body.get("_uiObservation") or {}).get("captureDisabled"):
                issues.append("ui_capture_unavailable")
            for cursor in following:
                if cursor in payload["seenCursors"]:
                    issues.append("cursor_cycle")
                elif cursor not in payload["queue"]:
                    payload["queue"].append(cursor)
            # A page with no rows at all ends the list immediately: over seven
            # live runs and 253 lists, no empty fetch was ever followed by a
            # productive one, and the retry cost 99 requests for no records.
            # A page that returns rows but nothing new is overlap, not
            # exhaustion -- a moving cursor still earns one more attempt.
            if (source == "native_ui" and following and
                    observation.get("stopReason") in self.UI_STOPS):
                issues.append(observation["stopReason"])
            elif source != "native_ui" and following and (empty or payload["emptyPages"] >= 2):
                issues.append("no_progress")
            if source in ("native", "native_ui") and params and gained:
                payload["template"]["validation"] = "pagination_confirmed"
                self.state.set_meta("nativeRepliesValidated" if replies else "nativeParentsValidated", True)
            if body.get("comment_count") is not None:
                payload["reportedCount"] = body["comment_count"]
            if issues:
                payload["stopReason"] = issues[0]
                status = "blocked"
            elif payload["queue"]:
                status = "pending"
            else:
                status = "done"
                if replies and isinstance(payload.get("expected"), int) and self._thread_count(job) < payload["expected"]:
                    payload["stopReason"] = "reported_count_gap"
                    status = "blocked"
            if replies and source == 'api':
                payload['serverExhaustionConfirmed'] = bool(
                    body.get('has_more_head_child_comments') is False and
                    body.get('has_more_tail_child_comments') is False and
                    not following and not payload['queue'])
            if (replies and source == "api" and status in ("done", "blocked")
                    and not self.config.is_newest_comments
                    and not payload.get('serverExhaustionConfirmed')
                    and not payload.get("uiReplyAttempted")
                    and isinstance(payload.get("expected"), int)
                    and self._thread_count(job) < payload["expected"]
                    and hasattr(self.ctx.session, "run")
                    and payload.get("stopReason") in (None, "reported_count_gap", "no_progress", "cursor_cycle", "missing_cursor")):
                previous_reason = payload.pop("stopReason", "exhausted")
                payload.update(source="native_ui", uiReplyAttempted=True, queue=[],
                               seenCursors=[], emptyPages=0, template={},
                               stopReason="awaiting_parent_pass",
                               restStopReason=previous_reason)
                status = "blocked"
            self.state.save_job(job, status=status)
            if saved_page:
                self.state.mark_pending_page_applied(job["id"])
        self.stats.pagination.append({"mediaId": scope, "parentCommentId": payload.get("parentId"),
                                      "source": source, "pages": payload["pages"], "uniqueItems": gained,
                                      "stopReason": payload.get("stopReason", status)})

    def _enrich(self, job):
        p = job["payload"]
        if p["type"] == "media":
            row = self.state.get_record(p["recordKey"])
            cached = self.state.cache_get(f"media:{p['code']}")
            raw = cached or self.api.media_by_shortcode(p["code"])
            mapped = map_post(raw, input_url=row["payload"].get("inputUrl"), preserve_unknowns=True)
            for field in POST_FIELDS:
                mapped.setdefault(field, None)
            with self.state.transaction():
                self.state.cache_set(f"media:{p['code']}", raw)
                self.state.upsert(mapped, scope=row["scope"], kind=row["kind"], source="media_info", observed=raw.get("_observed_at"),
                                  known=known_fields(raw, POST_FIELDS))
                self.state.mark_unavailable(p["recordKey"], mapped.keys(), "media_info")
                self._queue_owner(p["recordKey"], mapped)
                self.state.save_job(job, status="done")
        else:
            cache_key = f"owner:{p['ownerId']}"
            raw = self.state.cache_get(cache_key)
            if raw is None:
                by_id = str(p["ownerId"]).isdigit()
                raw = dict(self.api.user_by_id(p["ownerId"]) if by_id
                           else self.scrape.profile(p["username"]))
                if by_id:
                    raw["_profile_source"] = "user_by_id"
            raw.setdefault("_observed_at", time.time())
            profile_id = str(raw.get("id") or raw.get("pk") or "")
            if p["ownerId"].isdigit() and profile_id != p["ownerId"]:
                raise InstagramError("Owner identity changed; enrichment rejected")
            with self.state.transaction():
                self.state.cache_set(cache_key, raw)
                for row in self._owner_rows(p["ownerId"], p["username"]):
                    self._apply_owner(row["key"], raw)
                self.state.save_job(job, status="done")

    def _execute(self, job):
        try:
            self._budget()
            if job["kind"] == "enrich":
                self._enrich(job)
            else:
                self.ctx.operation_store = self.state
                self.ctx.operation_job_id = job["id"]
                self.ctx.operation_kind = job["kind"]
                try:
                    self._page(job)
                    self.consecutive_command_failures = 0
                finally:
                    self.ctx.operation_job_id = None
                    self.ctx.operation_kind = None
        except BroCommandOutcomeUnknown as exc:
            job["payload"]["stopReason"] = "command_outcome_unknown"
            self.state.save_job(job)
            self.storage.dataset.flush()
            raise FatalError(str(exc)) from exc
        except BroSessionError as exc:
            job["payload"]["stopReason"] = "vm_unavailable"
            self.state.save_job(job)
            self.storage.dataset.flush()
            raise FatalError(str(exc)) from exc
        except PageFetchReadTimeoutError as exc:
            job["payload"]["stopReason"] = "PageFetchReadTimeoutError"
            self.state.save_job(job, status="blocked")
            self.state.add_invocation_metric("pageFetchReadTimeouts", 1)
            self.storage.dataset.flush()
            self.consecutive_command_failures += 1
            if self.consecutive_command_failures >= 3:
                raise FatalError("Three consecutive reads failed or were cancelled") from exc
        except BroPayloadError as exc:
            job["payload"]["stopReason"] = "payload_download_failed"
            self.state.save_job(job, status="blocked")
            self.consecutive_command_failures += 1
            if self.consecutive_command_failures >= 3:
                raise FatalError("Three consecutive getbro reads failed") from exc
        except BroCommandError as exc:
            job["payload"]["stopReason"] = "BroCommandError"
            job["payload"]["commandError"] = exc.name
            self.state.save_job(job, status="blocked")
            self.consecutive_command_failures += 1
            if self.consecutive_command_failures >= 3:
                raise FatalError("Three consecutive getbro reads failed") from exc
        except (LoginRequiredError, RateLimitedError, ChallengeRequiredError) as exc:
            job["payload"]["stopReason"] = type(exc).__name__
            self.state.save_job(job)
            raise FatalError(str(exc)) from exc
        except FatalError as exc:
            job["payload"]["stopReason"] = ("observer_unconfirmed" if "observer_unconfirmed" in str(exc)
                else getattr(exc, "reason", None) or type(exc.__cause__ or exc).__name__)
            self.state.save_job(job)
            raise
        except InstagramError as exc:
            p = job["payload"]
            cursor_error = "cursor" in str(exc).lower() and any(k in str(exc).lower() for k in ("invalid", "expired", "reject", "changed"))
            if job["kind"] != "enrich" and p.get("source") == "native":
                p.update(source="rest", queue=[{}], seenCursors=[], pages=0, emptyPages=0)
                p.pop("first", None)
                p["nativeFallbackReason"] = type(exc).__name__
                p["nativeFallbackDetail"] = str(exc)[:300]
                self.state.save_job(job)
            elif cursor_error and not p.get("cursorReset"):
                p.update(queue=[{}], seenCursors=[], pages=0, emptyPages=0, cursorReset=True)
                self.state.save_job(job)
            else:
                p["stopReason"] = type(exc).__name__
                if job["kind"] == "enrich" and isinstance(exc, NotFoundError):
                    if p.get("recordKey"):
                        record = self.state.get_record(p["recordKey"])
                        self.state.mark_unavailable(p["recordKey"], record["fields"], "not_found")
                    elif p.get("type") == "owner":
                        missing = {"_unavailable": True}
                        self.state.cache_set(f"owner:{p['ownerId']}", missing)
                        for row in self._owner_rows(p["ownerId"], p["username"]):
                            self._apply_owner(row["key"], missing)
                    self.state.save_job(job, status="done")
                else:
                    self.state.save_job(job, status="blocked")
        self.storage.dataset.maybe_flush()
        traffic = getattr(self.ctx, "traffic", None)
        calls = getattr(self.ctx, "api_calls", 0)
        # Periodic accounting dumps are the HAR source's; with the observer
        # the network statistics are declared partial instead.
        if (traffic and not (self.config.results_type.value == "comments" and self.config.is_newest_comments)
                and not self._observer_active()
                and calls - self.last_traffic_poll >= self.TRAFFIC_POLL_EVERY):
            self.last_traffic_poll = calls
            operation = self.state.begin_operation(
                None, "har", hashlib.sha256(b"dump_har_logs").hexdigest(), 1)
            def har_event(status, fields):
                self.state.update_operation(
                    operation, status, session_id=getattr(self.ctx.session, "session_id", None),
                    command_id=fields.get("command_id"), phase=fields.get("phase"),
                    last_status=fields.get("last_status"))
                if status in ("recovering", "outcome_unknown"):
                    self.storage.dataset.flush()
            try:
                traffic.observe(self.ctx.session.run_one(
                    cmd.dump_har_logs(resource_types=self._har_resource_types),
                    retries=0, on_event=har_event))
                self.state.update_operation(operation, "applied", phase="telemetry",
                                            last_status="done")
            except (BroCommandError, BroPayloadError) as exc:
                self.state.update_operation(operation, "failed", phase=getattr(exc, "phase", None),
                                            last_status=getattr(exc, "last_status", None),
                                            error_type=type(exc).__name__)

    def drain(self):
        if self.config.collection_phase == "enrich":
            self._drain_enrichment()
            return
        # Only tasks carried over from an earlier invocation qualify. Newly
        # discovered direct tasks still wait for this document's parent pass.
        invocation = self.state.get_meta('invocation', 1)
        while True:
            row = self.state.db.execute("""SELECT id FROM jobs WHERE kind='replies'
                AND status='pending' AND json_extract(payload,'$.resumeDirectFirst')=?
                ORDER BY turn LIMIT 1""", (invocation,)).fetchone()
            if row is None:
                break
            job = self.state.get_job(row['id'])
            if job['scope'] in self.limited_scopes:
                job['payload'].pop('resumeDirectFirst', None)
                self.state.save_job(job, status=job['status'])
                continue
            self._execute(job)
        while True:
            if self.active_ui_scope is None:
                excluded = tuple(self.limited_scopes)
                parent = self.state.next_job("parents", exclude_scopes=excluded)
                candidate = parent or self.state.next_job("replies", exclude_scopes=excluded)
                if not candidate:
                    break
                self.active_ui_scope = candidate["scope"]
                self.state.set_meta("activeUiScope", self.active_ui_scope)
            did_work = False
            # Keep one post's DOM alive. Deep visible threads are drained by
            # its parent step; REST/cached reply work follows without switching
            # the browser to another publication.
            used = set()
            # A permalink thread navigates away from the post: it runs only
            # once the parent list of its post has nothing left to do (done
            # or blocked), never between two steps of the list's traversal
            # or its terminal audit.
            parents_busy = self.state.next_job(
                "parents", include_scopes=(self.active_ui_scope,)) is not None
            for kind in ("parents", "replies", "replies", "replies"):
                exclude = tuple(self.limited_scopes) if kind != "enrich" else ()
                ids = tuple(used) if kind == "replies" else ()
                job = self.state.next_job(
                    kind, include_scopes=(self.active_ui_scope,),
                    exclude_scopes=exclude, exclude_ids=ids,
                    exclude_sources=("direct_ui",) if kind == "replies" and parents_busy else ())
                if job:
                    if kind == "replies":
                        used.add(job["id"])
                    did_work = True
                    self._execute(job)
            if not did_work:
                self.active_ui_scope = None
                self.state.set_meta("activeUiScope", None)
        # Even blocked, limited or not-yet-discovered targets retain priority.
        # An explicit enrichment-only invocation may opt into those saved jobs.
        self._mark_count_gaps()
        unfinished = self.state.db.execute(
            "SELECT 1 FROM jobs WHERE kind IN ('parents','replies') AND status!='done' LIMIT 1"
        ).fetchone()
        undiscovered = self.state.db.execute(
            "SELECT 1 FROM targets WHERE status='pending' LIMIT 1"
        ).fetchone()
        if self.config.collection_phase == "all" and not unfinished and not undiscovered:
            self._drain_enrichment()

    def _drain_enrichment(self):
        budget = self.ctx.budget
        self.state.set_meta("enrichmentStopReason", None)
        while True:
            job = self.state.next_job("enrich")
            if not job:
                return
            if job["payload"].get("type") == "owner" and not self.config.expand_owners:
                # Preserve work from older checkpoints without sending a request.
                self.state.save_job(job, status="blocked")
                continue
            try:
                with budget.enrichment():
                    self._execute(job)
            except BudgetExceeded as exc:
                if exc.reason != "maxEnrichmentRequests":
                    raise
                self.state.set_meta("enrichmentStopReason", exc.reason)
                return

    def run(self, targets):
        self.state.set_meta("collectionStarted", True)
        self.apply_saved_pages()
        # A later explicit enrichment run can enable expandOwners even when the
        # original collection intentionally did not enqueue those lookups.
        if self.config.expand_owners:
            for row in self.state.db.execute("SELECT key,payload FROM records"):
                self._queue_owner(row["key"], json.loads(row["payload"]))
        if self.config.collection_phase != "enrich":
            self.register(targets)
        try:
            self.drain()
            # A getbro read timeout blocks the task for this pass (lite exits
            # answered first comment pages past 30 s from the 7th minute on,
            # 2026-09-25). Give each such task one more pass in this same
            # browser, cursor intact, instead of leaving it to a resume.
            reopened = self.state.reopen_command_failures(counter="reopenedInSession")
            if reopened:
                log.info("re-opening %d task(s) deferred by a getbro command failure for one "
                         "more pass in this session", len(reopened))
                self.state.set_meta("reopenedInSession",
                                    int(self.state.get_meta("reopenedInSession", 0)) + len(reopened))
                self.drain()
            # A confirmed browser cancellation has a known terminal outcome.
            # Other posts have already progressed; one later read of its saved
            # cursor is safe if the run still has time for a full read.
            remaining = self.ctx.budget.remaining()
            if (self.config.max_request_retries > 0 and
                    (remaining is None or remaining >= self.config.bro.read_timeout + 15)):
                reopened = self.state.reopen_page_fetch_timeouts(
                    max_retries=self.config.max_request_retries)
                if reopened:
                    log.info("retrying %d confirmed cancelled comment read(s) after other work", len(reopened))
                    self.state.add_invocation_metric("deferredPageFetchRetries", len(reopened))
                    self.drain()
        finally:
            if self.observer is not None:
                self.state.set_meta("observerSummary", self.observer.summary())
            if self._witness is not None:
                self.state.set_meta("harWitnessSummary", self._witness.summary())
        if self.config.collection_phase == "enrich":
            return
        self._mark_count_gaps()

    def _mark_count_gaps(self):
        if self.config.include_nested_comments and self.config.max_replies_per_comment is None:
            for row in self.state.db.execute("SELECT * FROM jobs WHERE kind='parents' AND status='done'"):
                job = {**dict(row), "payload": json.loads(row["payload"])}
                reported = job["payload"].get("reportedCount")
                if job["payload"].get("stopReason") == "date_floor":
                    continue  # the count covers comments outside the requested window
                if isinstance(reported, int) and reported > self.state.count(job["scope"]):
                    job["payload"]["stopReason"] = "reported_count_gap"
                    self.state.save_job(job, status="blocked")
