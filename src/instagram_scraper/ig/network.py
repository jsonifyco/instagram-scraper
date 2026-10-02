"""Reuse scoped JSON from getbro HAR responses without storing the HAR itself.

This is passive capture: no recorded request (including GraphQL POSTs) is
replayed. Headers, cookies and request bodies never enter output records.
"""
from __future__ import annotations

import base64
import binascii
import copy
import json
import logging
import re
import hashlib
import time
from datetime import datetime
from collections import OrderedDict
from typing import Any, Iterator
from urllib.parse import parse_qs, urlsplit

from ..bro import commands as cmd
from ..bro.session import _is_session_fault
from ..errors import BroCommandError, FatalError
from ..shortcode import shortcode_to_media_id, split_media_id
from .endpoints import web_retired

log = logging.getLogger(__name__)
MAX_BODY = 8 * 1024 * 1024
#: HAR types page fetch()/XHR land in; getbro files them under "other"
#: (har-since-probe 2026-09-15), the named types are kept in case it does not
APP_RESOURCE_TYPES = ("xhr", "fetch", "other")


def _objects(value: Any) -> Iterator[dict[str, Any]]:
    """Bound traversal of third-party response trees."""
    pending = [(value, 0)]
    visited = 0
    while pending and visited < 100_000:
        value, depth = pending.pop()
        visited += 1
        if depth > 32:
            continue
        if isinstance(value, dict):
            yield value
            pending.extend((v, depth + 1) for v in reversed(list(value.values()))
                           if isinstance(v, (dict, list)))
        elif isinstance(value, list):
            pending.extend((v, depth + 1) for v in reversed(value))


def _bodies(content: dict[str, Any]) -> Iterator[dict[str, Any]]:
    if not isinstance(content, dict):
        return
    text = content.get("text")
    if not isinstance(text, str) or len(text) > MAX_BODY:
        return
    if content.get("encoding") == "base64":
        try:
            text = base64.b64decode(text, validate=True).decode("utf-8")
        except (ValueError, UnicodeError, binascii.Error):
            return
    text = text.lstrip()
    if text.startswith("for (;;);"):
        text = text[len("for (;;);"):].lstrip()
    # Relay may stream several newline-separated JSON documents.
    try:
        values = [json.loads(text)]
    except (ValueError, RecursionError):
        values = []
        for line in text.splitlines():
            try:
                values.append(json.loads(line))
            except (ValueError, RecursionError):
                continue
    for value in values:
        if isinstance(value, dict) and not value.get("errors") and value.get("status") != "fail":
            yield value


def _comment_nodes(value: Any) -> Iterator[dict[str, Any]]:
    if isinstance(value, dict):
        value = value.get("edges") or []
    if not isinstance(value, list):
        return
    for item in value:
        if not isinstance(item, dict):
            continue
        node = item.get("node") if isinstance(item.get("node"), dict) else item
        if (node.get("pk") is not None or node.get("id") is not None) and (
            "text" in node or "giphy_media_info" in node or "media" in node
        ):
            yield node


def _variables(request: dict[str, Any]) -> dict[str, Any]:
    params = parse_qs(urlsplit(request.get("url", "")).query)
    post = request.get("postData") or {}
    if not isinstance(post, dict):
        return {}
    if "application/json" in post.get("mimeType", ""):
        try:
            payload = json.loads(post.get("text", ""))
            value = payload.get("variables")
            if isinstance(value, dict):
                return value
            params["variables"] = [value]
        except (ValueError, AttributeError):
            return {}
    else:
        params.update(parse_qs(post.get("text") or ""))
        for param in post.get("params") or []:
            if isinstance(param, dict) and param.get("name") == "variables":
                params["variables"] = [param.get("value")]
    try:
        value = json.loads((params.get("variables") or [""])[0])
        return value if isinstance(value, dict) else {}
    except (ValueError, TypeError):
        return {}


def _scope_matches(variables: dict[str, Any], media_id: str) -> bool:
    identities = []
    for obj in (variables, variables.get("data")):
        if not isinstance(obj, dict):
            continue
        for name in ("media_id", "mediaId"):
            if obj.get(name) is not None:
                try:
                    identities.append(str(split_media_id(obj[name])))
                except (ValueError, TypeError):
                    return False
        if isinstance(obj.get("shortcode"), str):
            try:
                identities.append(str(shortcode_to_media_id(obj["shortcode"])))
            except ValueError:
                return False
    return bool(identities) and set(identities) == {media_id}


def _not_served(url: str, response: dict[str, Any]) -> bool:
    """A REST read the logged-in web app no longer makes, answered with a page
    or nothing instead of JSON (``endpoints.WEB_RETIRED_PATHS``). That is
    "not served here", not a rejection of the account; JSON keeps counting."""
    if not web_retired(url):
        return False
    content = response.get("content") or {}
    text = content.get("text") or ""
    if content.get("encoding") == "base64":
        try:
            text = base64.b64decode(text).decode("utf-8", "replace")
        except (ValueError, binascii.Error):
            return False
    text = str(text).lstrip()
    return not text or text.startswith("<")


def _is_profile(obj: dict[str, Any]) -> bool:
    """A profile header object (what `web_profile_info`, `users/{id}/info` and
    the web app's ``PolarisProfilePageContentQuery`` return), not an author
    stub on a post or a comment: it carries the account's own counts."""
    return (isinstance(obj.get("username"), str) and (obj.get("pk") or obj.get("id")) is not None
            and any(k in obj for k in ("follower_count", "edge_followed_by"))
            and any(k in obj for k in ("following_count", "edge_follow"))
            and any(k in obj for k in ("media_count", "edge_owner_to_timeline_media")))


def _accounts(media: dict[str, Any]) -> set[str]:
    """The post's author and its co-authors (a collab post sits on each grid)."""
    owner = media.get("user") or media.get("owner") or {}
    names = {str(owner.get("username") or "").lower()} if isinstance(owner, dict) else set()
    names |= {str(c.get("username") or "").lower() for c in media.get("coauthor_producers") or []
              if isinstance(c, dict)}
    names.discard("")
    return names


def _profile_timeline(obj: dict[str, Any], variables: dict[str, Any]) -> tuple[str, list[dict[str, Any]]] | None:
    """A profile grid page (``PolarisProfilePostsQuery``): a connection whose
    every post names one account as author or co-author. @nasa's grid holds
    collab posts authored by other accounts (2026-09-29), so authorship alone
    is not the test; the Tagged tab and the home feed share no such account.
    The request's own ``username`` variable settles a tie."""
    edges = obj.get("edges")
    if not isinstance(edges, list) or not edges:
        return None
    nodes = [edge.get("node") for edge in edges if isinstance(edge, dict)]
    if not nodes or any(not isinstance(n, dict) or not isinstance(n.get("code") or n.get("shortcode"), str)
                        for n in nodes):
        return None
    shared: set[str] | None = None
    for node in nodes:
        shared = _accounts(node) if shared is None else shared & _accounts(node)
    if not shared:
        return None
    hint = str(variables.get("username") or "").lower()
    if hint in shared:
        return hint, nodes
    return (shared.pop(), nodes) if len(shared) == 1 else None


def media_is_complete(raw: dict[str, Any]) -> bool:
    """Sparse tiles must still get a media lookup; zero metrics are valid."""
    try:
        return _media_is_complete(raw)
    except (TypeError, ValueError, AttributeError):
        return False


def _media_is_complete(raw: dict[str, Any]) -> bool:
    owner = raw.get("user") or raw.get("owner") or {}
    code = raw.get("code") or raw.get("shortcode")
    pk = raw.get("pk") or raw.get("id")
    if not isinstance(code, str) or pk is None or split_media_id(pk) != shortcode_to_media_id(code):
        return False
    if not owner.get("username") or not (raw.get("taken_at") or raw.get("taken_at_timestamp")):
        return False
    if raw.get("media_type"):
        required = ("caption", "like_count", "comment_count", "image_versions2")
        if not all(k in raw for k in required) or raw.get("like_count") is None or raw.get("comment_count") is None:
            return False
        if raw["media_type"] == 2 and not raw.get("video_versions"):
            return False
        if raw["media_type"] == 8:
            children = raw.get("carousel_media") or []
            if not children or len(children) < (raw.get("carousel_media_count") or len(children)):
                return False
            if any(not c.get("image_versions2") or
                   (c.get("media_type") == 2 and not c.get("video_versions")) for c in children):
                return False
        return bool((raw.get("image_versions2") or {}).get("candidates"))
    required = ("edge_media_to_caption", "display_url", "edge_media_preview_like")
    if not all(k in raw for k in required):
        return False
    if (raw.get("edge_media_preview_like") or {}).get("count") is None:
        return False
    if not any((raw.get(k) or {}).get("count") is not None for k in (
        "edge_media_to_parent_comment", "edge_media_to_comment")):
        return False
    if raw.get("is_video") and not raw.get("video_url"):
        return False
    if raw.get("__typename") == "GraphSidecar":
        children = (raw.get("edge_sidecar_to_children") or {}).get("edges") or []
        if not children or any(not (c.get("node") or {}).get("display_url") or
                               ((c.get("node") or {}).get("is_video") and
                                not c["node"].get("video_url")) for c in children):
            return False
    return True


class NetworkCapture:
    """Target-local collection. Only DOM-observed media codes are retained."""

    def __init__(self, *, media_id: str | None = None, newest_first: bool = False,
                 stop_on_rejection: bool = False, rejection_since: float | None = None,
                 fingerprint_store=None, defer_claims: bool = False):
        self.media_id = str(split_media_id(media_id)) if media_id is not None else None
        self.newest_first = newest_first
        self.media: dict[str, dict[str, Any]] = {}
        self.comments: dict[str, dict[str, Any]] = {}
        self.first_comment_page: dict[str, Any] | None = None
        self.reply_pages: dict[str, dict[str, Any]] = {}
        self.reply_latest_pages: dict[str, dict[str, Any]] = {}
        self._replies: dict[str, dict[str, dict[str, Any]]] = {}
        self.disabled = False
        self.polls = 0
        self._seen = OrderedDict()
        self._media_cache = OrderedDict()
        self.native = {}
        self.native_latest = {}
        self.native_versions = {}
        self._native_times = {}
        self.responses_seen = 0
        self.native_graphql_responses = 0
        self.native_diagnostics = {}
        self.stop_on_rejection = stop_on_rejection
        self.rejection_since = rejection_since
        self.fingerprint_store = fingerprint_store
        #: With deferred claims a decoded response is only *checked* against
        #: the store during ingest; its fingerprint is written by
        #: :meth:`commit_claims` inside the same transaction that saves the
        #: pages decoded from it. A crash between the two therefore re-reads
        #: the response instead of losing it.
        self.defer_claims = bool(defer_claims)
        self._pending_claims: dict[str, float] = {}
        #: every comment id decoded from a scoped comment operation, whatever
        #: the source; the observer verification compares two of these sets
        self.seen_comment_ids: set[str] = set()
        #: profile header objects the page received, by lowercase username;
        #: the latest observed one wins (a logged-in web session is served
        #: its profiles only this way, see ``page.read_app_profile``)
        self.profiles: dict[str, dict[str, Any]] = {}
        self._profile_times: dict[str, float] = {}
        #: each account's grid posts (its own and its collabs, see
        #: ``_profile_timeline``) in the order the page received them
        #: (pinned first, then newest)
        self.timelines: dict[str, OrderedDict] = {}

    def poll(self, session, *, codes: set[str] | None = None, on_event=None,
             resource_types=None, since=None):
        """One dump without a time cutoff. ``resource_types`` narrows the export to those
        types; ``since`` (Unix seconds, inclusive, by request start) is only
        for bounded diagnostic reads -- the collector never advances it on
        its own, because a request that started before an earlier export may
        finish after it."""
        if self.disabled:
            return
        try:
            payload = session.run_one(cmd.dump_har_logs(since=since, resource_types=resource_types),
                                      retries=0, on_event=on_event)
        except BroCommandError as exc:
            if _is_session_fault(exc):
                raise
            # HAR is optional. Do not create another browser to retry it.
            log.info("HAR capture unavailable (%s); continuing through the API", exc.name)
            self.disabled = True
            return
        self.polls += 1
        self.ingest(payload, codes=codes or set())
        return payload

    def ingest(self, payload: Any, *, codes: set[str] | None = None) -> None:
        if not isinstance(payload, dict):
            return
        har = payload.get("har_logs", payload)
        if not isinstance(har, dict):
            return
        archive_log = har.get("log") or {}
        if not isinstance(archive_log, dict):
            return
        entries = archive_log.get("entries") or []
        if not isinstance(entries, list):
            return
        self.ingest_entries(entries, codes=codes)

    def ingest_entries(self, entries: list, *, codes: set[str] | None = None) -> None:
        """Parse HAR-shaped entries from any source: a cumulative HAR dump or
        the in-page observer's queue (see :mod:`.observer`)."""
        # HAR can be cumulative; retain only scoped parsed data, never entries.
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            request, response = entry.get("request") or {}, entry.get("response") or {}
            if not isinstance(request, dict) or not isinstance(response, dict):
                continue
            if not isinstance(request.get("url"), str):
                continue
            try:
                url = urlsplit(request["url"])
            except ValueError:
                continue
            if url.hostname not in {"www.instagram.com", "instagram.com", "i.instagram.com"}:
                continue
            if not (url.path.startswith("/api/v1/") or url.path.rstrip("/") in {"/graphql/query", "/api/graphql"}):
                continue
            try:
                observed_at = datetime.fromisoformat(entry.get("startedDateTime", "").replace("Z", "+00:00")).timestamp()
            except (ValueError, TypeError, AttributeError):
                observed_at = time.time()
            if self.stop_on_rejection and (self.rejection_since is None or observed_at >= self.rejection_since):
                if response.get("status") in (401, 403, 429) and not _not_served(request["url"], response):
                    raise FatalError(f"Browser background Instagram request rejected ({response['status']}); stopping")
                try:
                    error_body = json.loads((response.get("content") or {}).get("text") or "{}")
                except (ValueError, TypeError):
                    error_body = {}
                if isinstance(error_body, dict):
                    message = str(error_body.get("message") or "").lower()
                    if error_body.get("require_login") or error_body.get("challenge") or any(
                        s in message for s in ("challenge_required", "checkpoint_required", "login_required", "please wait a few minutes")):
                        raise FatalError("Browser background Instagram request required account action; stopping")
            if response.get("status") != 200:
                continue
            # HAR and the page observer describe the same response with
            # different headers/timestamps. Count that page only once.
            params = parse_qs((request.get("postData") or {}).get("text") or "")
            content = response.get("content") or {}
            text_body = content.get("text") or ""
            if content.get("encoding") == "base64":
                try:
                    text_body = base64.b64decode(text_body).decode("utf-8")
                except (ValueError, UnicodeError):
                    pass
            namespace = self.fingerprint_store.get_meta("sessionId") if self.fingerprint_store else None
            signature = hashlib.sha256(json.dumps({
                "session": namespace, "method": request.get("method"), "path": url.path.rstrip("/"),
                "query": parse_qs(url.query), "variables": _variables(request),
                "doc": params.get("doc_id"), "operation": params.get("fb_api_req_friendly_name"),
                "body": text_body}, sort_keys=True).encode()).hexdigest()
            metadata_only = False
            if self.fingerprint_store is not None and self.defer_claims:
                if signature in self._pending_claims or signature in self._seen:
                    continue
                metadata_only = self.fingerprint_store.has_har_response(signature)
                if metadata_only:
                    # Rehydrate live templates on same-VM resume without
                    # counting already committed response pages again.
                    self._seen[signature] = None
                else:
                    self._pending_claims[signature] = observed_at
            elif self.fingerprint_store is not None:
                if not self.fingerprint_store.claim_har_response(signature, observed_at):
                    continue
            else:
                if signature in self._seen:
                    continue
                self._seen[signature] = None
                if len(self._seen) > 5000:
                    self._seen.popitem(last=False)
            self.responses_seen += 1
            if request.get("method") == "POST" and url.path.rstrip("/") in {"/graphql/query", "/api/graphql"}:
                self.native_graphql_responses += 1
            try:
                observed_at = datetime.fromisoformat(entry.get("startedDateTime", "").replace("Z", "+00:00")).timestamp()
            except (ValueError, TypeError):
                observed_at = time.time()
            request_variables = _variables(request)
            for body in _bodies(response.get("content") or {}):
                for item in _objects(body):
                    if (item.get("pk") is not None or item.get("id") is not None) and ("text" in item or "code" in item or "shortcode" in item):
                        item["_observed_at"] = observed_at
                for raw in _objects(body):
                    code = raw.get("code") or raw.get("shortcode")
                    if isinstance(code, str) and media_is_complete(raw):
                        self._media_cache[code] = copy.deepcopy(raw)
                        self._media_cache.move_to_end(code)
                        if len(self._media_cache) > 500:
                            self._media_cache.popitem(last=False)
                    if _is_profile(raw):
                        name = raw["username"].lower()
                        if observed_at >= self._profile_times.get(name, float("-inf")):
                            self.profiles[name] = copy.deepcopy(raw)
                            self._profile_times[name] = observed_at
                    timeline = _profile_timeline(raw, request_variables)
                    if timeline:
                        owner, nodes = timeline
                        rows = self.timelines.setdefault(owner, OrderedDict())
                        for node in nodes:
                            node_code = node.get("code") or node.get("shortcode")
                            if node_code not in rows:
                                if len(rows) < 60:
                                    rows[node_code] = copy.deepcopy(node)
                            elif media_is_complete(node) and not media_is_complete(rows[node_code]):
                                rows[node_code] = copy.deepcopy(node)
                    if self.media_id and not self.newest_first and not metadata_only:
                        pk = raw.get("pk") or raw.get("id")
                        try:
                            matches = code and pk is not None and str(split_media_id(pk)) == self.media_id
                        except (ValueError, TypeError):
                            matches = False
                        if matches:
                            for key in ("preview_comments", "comments", "edge_media_to_parent_comment", "edge_media_to_comment"):
                                self._add_comments(raw.get(key))
                if self.media_id:
                    if not metadata_only:
                        self._ingest_comments(request, body)
                    from .native_comments import observe
                    for template, connection in observe(
                            request, body, self.media_id, self.native_diagnostics):
                        if template["parentId"] or not self.newest_first or template["sort"] == "recent":
                            key = template["parentId"] or "parents"
                            # Cumulative HAR need not be ordered; an older
                            # response must not replace the current page_info.
                            if observed_at >= self._native_times.get(key, 0):
                                self.native_latest[key] = (template, connection)
                                self._native_times[key] = observed_at
                                if not metadata_only:
                                    self.native_versions[key] = self.native_versions.get(key, 0) + 1
                            previous = self.native.get(key)
                            if previous is None or template.get("observedCursor") is None:
                                self.native[key] = (template, connection)
                            if metadata_only:
                                # Already committed: the template is live
                                # again, its rows are not a new page.
                                continue
                            if template["parentId"]:
                                rows = self._replies.setdefault(key, {})
                                for raw in _comment_nodes(connection):
                                    rows[str(raw.get("pk") or raw.get("id"))] = copy.deepcopy(raw)
                                    self.seen_comment_ids.add(str(raw.get("pk") or raw.get("id")))
                            else:
                                for raw in _comment_nodes(connection):
                                    self.seen_comment_ids.add(str(raw.get("pk") or raw.get("id")))
        for code in codes or set():
            if code in self._media_cache:
                self.media[code] = self._media_cache[code]
        # Child pages may precede their parent in the archive. Attach only to
        # parents belonging to this media, after the full pass is parsed.
        for parent_id, replies in self._replies.items():
            parent = self.comments.get(parent_id)
            if parent is not None:
                existing = list(_comment_nodes(parent.get("preview_child_comments")
                                               or parent.get("edge_threaded_comments")))
                merged = {str(c.get("pk") or c.get("id")): c for c in existing}
                merged.update(replies)
                parent["preview_child_comments"] = list(merged.values())

    def _add_comments(self, rows: Any) -> None:
        for raw in _comment_nodes(rows):
            key = str(raw.get("pk") or raw.get("id"))
            self.seen_comment_ids.add(key)
            tagged = copy.deepcopy(raw)
            tagged["dataSource"] = "network"
            for name in ("preview_child_comments", "edge_threaded_comments"):
                for reply in _comment_nodes(tagged.get(name)):
                    reply["dataSource"] = "network"
            # Repeated HAR dumps must not multiply comments. Prefer the latest
            # non-null fields; a null sparse response must not erase good data.
            previous = self.comments.get(key, {})
            old_previews = previous.get("preview_child_comments") or previous.get("edge_threaded_comments")
            new_previews = tagged.get("preview_child_comments") or tagged.get("edge_threaded_comments")
            previews = {str(c.get("pk") or c.get("id")): c
                        for c in _comment_nodes(old_previews)}
            previews.update({str(c.get("pk") or c.get("id")): c
                             for c in _comment_nodes(new_previews)})
            self.comments[key] = {**previous,
                                  **{k: v for k, v in tagged.items() if v is not None}}
            if previews:
                self.comments[key]["preview_child_comments"] = list(previews.values())

    def _ingest_comments(self, request: dict[str, Any], body: dict[str, Any]) -> None:
        url = urlsplit(request.get("url") or "")
        match = re.fullmatch(r"/api/v1/media/(\d+)/comments/", url.path)
        params = parse_qs(url.query, keep_blank_values=True)
        child = re.fullmatch(r"/api/v1/media/(\d+)/comments/(\d+)/child_comments/", url.path)
        if child and child[1] == self.media_id and request.get("method", "GET") == "GET":
            rows = self._replies.setdefault(child[2], {})
            for raw in _comment_nodes(body.get("child_comments")):
                rows[str(raw.get("pk") or raw.get("id"))] = {**copy.deepcopy(raw), "dataSource": "network"}
                self.seen_comment_ids.add(str(raw.get("pk") or raw.get("id")))
            if isinstance(body.get("child_comments"), list):
                # Preserve the cursor contract of the latest REST reply page.
                # Direct-comment UI expansion uses this endpoint, and rows
                # alone are insufficient to continue it safely.
                self.reply_latest_pages[child[2]] = copy.deepcopy(body)
                head = "min_id" in params
                cursor_key = ("next_min_child_cursor" if head
                              else "next_max_child_cursor")
                more_key = ("has_more_head_child_comments" if head
                            else "has_more_tail_child_comments")
                requested = (params.get("min_id" if head else "max_id") or [None])[-1]
                next_cursor = body.get(cursor_key)
                has_next = body.get(more_key)
                # The tail ending says nothing about the head. Missing flags
                # likewise are not affirmative evidence of server exhaustion.
                if (body.get("has_more_head_child_comments") is True or
                        body.get("has_more_tail_child_comments") is True or
                        body.get("next_min_child_cursor") or body.get("next_max_child_cursor")):
                    has_next = True
                elif (body.get("has_more_head_child_comments") is False and
                      body.get("has_more_tail_child_comments") is False):
                    has_next = False
                else:
                    has_next = None
                template = {
                    "parentId": child[2], "transport": "rest",
                    "direction": "backward" if head else "forward",
                    "observedCursor": requested,
                    "observedEndCursor": next_cursor,
                    "observedHasNext": has_next,
                }
                self.native_latest[child[2]] = (template, {})
                self._native_times[child[2]] = time.time()
                self.native_versions[child[2]] = self.native_versions.get(child[2], 0) + 1
            if not any(k in params for k in ("min_id", "max_id")) and isinstance(body.get("child_comments"), list):
                self.reply_pages[child[2]] = copy.deepcopy(body)
            return
        if match and match[1] == self.media_id and request.get("method", "GET") == "GET":
            if self.newest_first and params.get("sort_order") != ["recent"]:
                return
            self._add_comments(body.get("comments"))
            if not any(k in params for k in ("min_id", "max_id")) and isinstance(body.get("comments"), list):
                self.first_comment_page = copy.deepcopy(body)
            return
        variables = _variables(request)
        nested = variables.get("data") or {}
        parent_id = (variables.get("comment_id") or variables.get("parent_comment_id")
                     or (nested.get("comment_id") or nested.get("parent_comment_id")
                         if isinstance(nested, dict) else None))
        if (self.newest_first and parent_id is None) or not _scope_matches(variables, self.media_id):
            return
        # Connection responses without an enclosing media object are accepted
        # only when request variables identify this exact post.
        data = body.get("data") or {}
        if not isinstance(data, dict):
            return
        for key, value in data.items():
            if "comments" in key and isinstance(value, dict) and "edges" in value:
                if parent_id is not None:
                    replies = self._replies.setdefault(str(parent_id), {})
                    for raw in _comment_nodes(value):
                        replies[str(raw.get("pk") or raw.get("id"))] = {**copy.deepcopy(raw), "dataSource": "network"}
                        self.seen_comment_ids.add(str(raw.get("pk") or raw.get("id")))
                elif "child" not in key and "threaded" not in key:
                    self._add_comments(value)

    def commit_claims(self, store=None) -> int:
        """Write the deferred fingerprints; call inside the saving transaction."""
        store = store or self.fingerprint_store
        pending, self._pending_claims = self._pending_claims, {}
        if store is None:
            return 0
        for signature, observed_at in pending.items():
            store.claim_har_response(signature, observed_at)
        return len(pending)

    def reply_parents(self):
        """Parents whose replies this capture is holding for a reply job to drain."""
        return [parent for parent, rows in self._replies.items() if rows]

    def reply_rows(self, parent_id):
        """Already observed replies for exactly one parent; no parent DOM required."""
        return list(self._replies.get(str(parent_id), {}).values())

    def clear_reply_rows(self, parent_id):
        self._replies.pop(str(parent_id), None)
