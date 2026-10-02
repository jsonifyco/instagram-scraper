"""Run orchestration: input -> targets -> browser sessions -> dataset.

One :class:`ScraperRun` owns the whole lifecycle:

1. validate the input and build the target list (direct URLs + search hits),
2. open one or more getbro sessions and bootstrap an Instagram context in each,
3. dispatch every target to the scraper registered for the `resultsType`,
4. stream records into the dataset and write a run summary,
5. tear the sessions down, whatever happened, so nothing keeps billing.

Several sessions run at once in two cases: anonymous workers
(``broConcurrency``) and several Instagram accounts (``sessionCookies`` plus
``sessionCookiesList``), one browser per account. In the default mode they
share one target queue (:mod:`.pool`); in complete mode each account
collects its own share of the targets into its own checkpoint
(:meth:`ScraperRun._run_shares`).
"""

from __future__ import annotations

import collections
import copy
import json
import logging
import os
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from urllib.parse import urlsplit
from dataclasses import dataclass, field
from typing import Any

from .bro import SDK_VERSION, make_client
from .bro.client import untag_steps
from .bro.client import step_data
from .bro.session import BroSession
from .errors import (BroCommandError, BroCommandOutcomeUnknown, BroPayloadError,
                     BroSessionError, BroTimeoutError,
                     FatalError, InputError, InstagramError, NotFoundError, ScraperError,
                     RateLimitedError, ChallengeRequiredError, LoginRequiredError, PageFetchReadTimeoutError)
from .ig.ai import AiExtractor
from .ig.api import InstagramApi
from .ig import native_comments, page as ig_page
from .ig import endpoints as ig_ep
from .ig.context import page_fetch_key, wait_page_fetch_slot, IgContext
from .input_model import HAR_FILTER_RESOURCE_TYPES, ScraperInput, cookie_viewer_id
from .pool import Lane, LaneLost, ProfileSeeds, TargetQueue
from .scrapers import ScrapeContext, Stats, hits_to_targets, scraper_for
from . import shares as share_plan
from .scrapers.search import SearchScraper
from .storage import KeyValueStore, RunStorage
from .urls import Target, TargetType, parse_targets, resolve_user_id
from .budget import RunBudget, BudgetExceeded, ShareBudget
from .preflight import verify_route
from .state import PersistentStorage
from .traffic import Traffic
from .bro import commands as cmd

log = logging.getLogger(__name__)


@dataclass
class RunResult:
    """What a finished run produced."""

    items: list[dict[str, Any]]
    stats: Stats
    billing: dict[str, Any]
    dataset_path: str | None = None
    summary_path: str | None = None
    #: the ranked search hits that became targets (``search`` runs), as
    #: mapped records; also written to ``SEARCH_HITS`` in the key-value store
    search_hits: list[dict[str, Any]] = field(default_factory=list)
    #: what each browser of a parallel run did (``Lane.report``), or each
    #: account's share of a complete-mode run with several accounts
    lanes: list[dict[str, Any]] = field(default_factory=list)

    def __len__(self) -> int:
        return self.stats.items


class ScraperRun:
    """Executes one scrape end to end."""

    def __init__(self, config: ScraperInput, *, storage: RunStorage | None = None) -> None:
        self.config = config
        self.client = make_client(
            config.bro.api_key,
            base_url=config.bro.base_url,
            dispatch_timeout=config.bro.dispatch_timeout,
        )
        self.client.drop_observation_once = (
            os.getenv("BRO_TEST_DROP_COMMAND_OBSERVATION_ONCE") == "1")
        self.storage = storage
        self.stats = Stats()
        self.sessions: list[BroSession] = []
        self._lock = threading.Lock()
        self._billing: dict[str, Any] = {"total": 0.0, "sessions": []}
        #: records buffered here when no storage backend was supplied
        self._collected: list[dict[str, Any]] = []
        self._shared_context: ScrapeContext | None = None
        self.budget = RunBudget(config.max_api_requests, config.max_run_seconds,
                                max_enrichment_requests=config.max_enrichment_requests)
        self.client.time_remaining = self.budget.remaining
        self.preflight: list[dict[str, Any]] = []
        self._targets: list[Target] = []
        self.search_hits: list[dict[str, Any]] = []
        #: accepted Apify inputs that this scraper cannot honour
        self.unsupported_inputs: list[str] = []
        self.stop_reason = None
        #: the browsers of a parallel run (anonymous workers or accounts)
        self.lanes: list[Lane] = []
        self._queue: TargetQueue | None = None
        #: what stopped every lane at once (the run budget)
        self._pool_stop: BaseException | None = None
        #: targets left in the queue because every lane was lost
        self.not_scraped: list[str] = []
        #: account index -> its browser's context (account 0 is ``_shared_context``)
        self._account_contexts: dict[int, ScrapeContext] = {}
        #: profile ids read anonymously before cookies, shared by account lanes
        self._seeds = ProfileSeeds()
        #: complete mode with several accounts: one share per account
        self.shares: list[dict[str, Any]] = []
        #: a share of such a run: returns the next post for this account's
        #: browser, or None when no post is left (set by the parent run)
        self.feed = None
        self._cleanups = 0
        self._saved_time_remaining = None
        self.traffic = Traffic()
        self.traffic.partial = config.collection_mode == "complete" and (
            config.response_source == "observer" or
            config.results_type.value == "comments" and config.is_newest_comments)
        #: ``har_filtered``: every export of this run (including the closing
        #: accounting dump) carries the same resource-type filter, and the
        #: traffic statistics say so
        self.har_resource_types = (list(HAR_FILTER_RESOURCE_TYPES)
                                   if config.collection_mode == "complete"
                                   and config.response_source != "har" else None)
        self.traffic.resource_types = self.har_resource_types
        self.started_at = time.time()
        self.starting_count = 0
        if config.collection_mode == "complete":
            if storage is not None and not isinstance(storage, PersistentStorage):
                raise InputError("complete mode requires PersistentStorage (or omit storage)")
            self.storage = storage or PersistentStorage(config.resume or config.output_dir,
                                                        fmt=config.output_format, resume=bool(config.resume),
                                                        name=config.dataset_name)
            self.storage.state.bind(config, resume=bool(config.resume))
            self.starting_count = self.storage.state.count()
            self.storage.state.set_meta("firstResultThisInvocation", None)
            # These markers describe the current process invocation.  A resumed
            # checkpoint retains cumulative recovery history, but must not make
            # the new run look as if its first failure happened at record zero.
            self.storage.state.set_meta("recordsAtFirstCommandFailure", None)
            self.storage.state.set_meta("recoveredCommandsThisInvocation", 0)

    # ------------------------------------------------------------- targets --

    def build_targets(self) -> list[Target]:
        """Direct URLs first, then anything the search query resolves to."""
        targets, failures = parse_targets(self.config.direct_urls)
        for url, reason in failures:
            log.warning("skipping %s: %s", url, reason)
            self.stats.record_failure(url, InputError(reason))

        if self.config.search.strip() and not self.config.direct_urls:
            targets.extend(self._search_targets())

        if not targets:
            raise InputError(
                "nothing to scrape: every directUrl was rejected and the "
                "search query resolved to no targets"
            )

        seen: set[tuple[str, str]] = set()
        unique: list[Target] = []
        for target in targets:
            # Shortcodes are case sensitive; usernames are not.
            key = (target.type.value, target.key if target.type.is_media else target.key.lower())
            if key not in seen:
                seen.add(key)
                unique.append(target)
        log.info("resolved %d target(s): %s", len(unique),
                 ", ".join(str(t) for t in unique[:10])
                 + (" ..." if len(unique) > 10 else ""))
        return unique

    def _search_targets(self) -> list[Target]:
        """Run the discovery step in its own short-lived session."""
        shared = bool(self.config.session_cookies) or self.config.collection_mode == "complete"
        session = None
        try:
            if shared:
                scrape = self._authenticated_context()
            else:
                session = self._new_session(label="search")
                scrape = self._make_context(session)
            searcher = SearchScraper(scrape)
            found: list[Target] = []
            for query in searcher.queries():
                hits = searcher.resolve(query)
                log.info("search %r -> %d %s hit(s)", query, len(hits),
                         self.config.search_type.value)
                # Hits become targets: re-scraping them through the configured
                # resultsType yields richer records than the search response
                # itself, and avoids emitting each entry twice into the
                # dataset. The hits themselves are kept for the run output.
                self.search_hits.extend(hits)
                found.extend(hits_to_targets(hits, self.config.search_type))
            return found
        except FatalError:
            raise
        except ScraperError as exc:
            log.error("search discovery failed: %s", exc)
            self.stats.record_failure(f"search:{self.config.search}", exc)
            return []
        finally:
            if session is not None:
                self._retire(session)

    # ------------------------------------------------------------ execution --

    def pin_route(self) -> None:
        """Pin the browser's exit route without proxy hunting.

        A run with cookies is pinned to DE: an account must keep reaching
        Instagram from the same country. An anonymous run has no such
        constraint and keeps the input's ``proxyCountry``; without one, the
        exit is getbro's default country (UK).

        On a DE route the city defaults to Frankfurt, the user-selected
        route. An explicit empty ``proxyCity`` opts out of the city pin (any
        DE exit) -- allowed for bounded checks while the Frankfurt pool is
        down. The opt-out stays ``""`` in the config: it is what the
        checkpoint saves as the run's input, and a resume must reproduce the
        same route rather than fall back to Frankfurt because ``None`` means
        "not chosen".
        """
        proxy = self.config.proxy
        if self.config.account_jars():
            proxy.country = "DE"
        elif proxy.country is not None and not str(proxy.country).strip():
            proxy.country = None
        if proxy.country:
            proxy.country = str(proxy.country).upper()
        if proxy.city is None:
            if proxy.country == "DE":
                proxy.city = "Frankfurt"
        elif not str(proxy.city).strip():
            proxy.city = ""

    def route_label(self) -> str:
        """``DE/Frankfurt``, ``DE/any city`` or ``getbro default country (UK)`` for the log."""
        proxy = self.config.proxy
        if not proxy.country:
            return f"getbro default country (UK){'/' + proxy.city if proxy.city else ''}"
        return f"{proxy.country}/{proxy.city or 'any city'}"

    def run(self) -> RunResult:
        """Scrape everything and return the collected records."""
        self.config.validate()
        self.pin_route()
        if self.config.collection_mode == "complete":
            self.config.bro.concurrency = 1
            self.config.proxy.policy = "full"
        if self.config.enhance_user_search_with_facebook_page:
            # Apify enriches user search hits with the linked Facebook page;
            # that needs a Facebook lookup this scraper does not make. The
            # flag is accepted so Apify inputs run unchanged, and the run
            # output says plainly that it had no effect.
            log.warning("enhanceUserSearchWithFacebookPage is not supported: search hits are "
                        "returned without Facebook page data")
            self.unsupported_inputs.append("enhanceUserSearchWithFacebookPage")
        if self.config.expand_owners and self.config.collection_mode != "complete":
            raise InputError("expandOwners requires collectionMode complete")
        for note in self.config.pin_session_to_one_ip():
            log.info("authenticated run: %s", note)
        self.budget.max_ip_rotations = self.config.max_ip_rotations
        log.info("Using %s; %s", self.route_label(),
                 "one exit IP per account" if self.config.account_jars() else
                 f"up to {self.config.max_ip_rotations} bro session swap(s) when Instagram walls off an exit IP")
        if not self.config.bro.skip_balance_check:
            balance = self.client.check_balance()
            log.info("getbro balance: $%.2f", balance)

        if self.storage:
            self.storage.save_input(self.config.to_dict())
            if isinstance(self.storage, PersistentStorage):
                self.storage.state.set_meta("input", self.config.to_dict())
                self.storage.dataset.start_exporter()

        try:
            if self._sharded():
                # Every share keeps its own checkpoint; this run's checkpoint
                # keeps the plan and the merged dataset.
                self._targets = self.build_targets()
                self._run_shares(self._targets)
                return self._finish()
            targets = [] if self.config.collection_phase == "enrich" else self.build_targets()
            self._targets = targets
            if self.config.collection_mode == "complete":
                from .complete import CompleteCollector
                if self.feed is not None:
                    # One account's share of a split run: only the posts it
                    # has taken so far belong to this checkpoint.
                    taken = {row[0] for row in self.storage.state.db.execute("SELECT id FROM targets")}
                    targets = [t for t in targets if t.url in taken]
                for target in targets:
                    self.storage.state.db.execute("INSERT OR IGNORE INTO targets VALUES(?,?)", (target.url, "pending"))
                scrape = self._authenticated_context()
                collector = CompleteCollector(scrape, self.storage)
                collector.apply_saved_pages()
                if self.config.resume:
                    self._recover_checkpoint_command(scrape.ctx.session, ctx=scrape.ctx)
                    collector.apply_saved_pages()
                collector.run(targets)
                while self.feed is not None:
                    # The next post goes to whichever account's browser is
                    # free first, in the same browser and checkpoint.
                    target = self.feed()
                    if target is None:
                        break
                    self.storage.state.db.execute("INSERT OR IGNORE INTO targets VALUES(?,?)",
                                                  (target.url, "pending"))
                    targets.append(target)
                    collector.run([target])
                coverage = self.storage.state.coverage()
                partial = coverage["pendingByKind"] or coverage["traversal"] != "source_exhausted"
                self.stats.targets_partial = len(targets) if partial else 0
                self.stats.targets_done = 0 if partial else len(targets)
                self._flush_checkpoint()
                self._shutdown()
                return self._finish()
            lanes = self._plan_lanes(targets)
            if len(lanes) == 1:
                self._run_serial(targets)
            else:
                self._run_pool(targets, lanes)
        except BudgetExceeded as exc:
            self.stop_reason = exc.reason
            if not getattr(exc, "partial_counted", False):
                self.stats.targets_partial += 1
            self._flush_checkpoint()
            self._shutdown()
            return self._finish()
        except BaseException as exc:
            # Persist partial JSON/CSV as well as JSONL before propagating an
            # interrupted/checkpointed run. Callers still see the real error.
            if self._queue is not None:
                self._queue.close()
            if not getattr(exc, "share_recorded", False):
                self.stats.record_failure("run", exc)
            cause = exc.__cause__
            self.stop_reason = ("command_outcome_unknown"
                                if isinstance(cause or exc, BroCommandOutcomeUnknown)
                                else "observer_unconfirmed" if "observer_unconfirmed" in str(exc)
                                else type(cause or exc).__name__)
            self._flush_checkpoint()
            self._shutdown()
            self._finish()
            raise
        finally:
            self._shutdown()

        return self._finish()

    def _run_serial(self, targets: list[Target]) -> None:
        if self.config.session_cookies:
            scrape = self._authenticated_context()
        else:
            session = self._new_session(label="main")
            scrape = self._make_context(session)
        for target in targets:
            self._scrape_target(scrape, target)

    def _authenticated_context(self, account: int = 0) -> ScrapeContext:
        """The browser of one account (the first by default), started once."""
        if account == 0:
            if self._shared_context is None:
                self._shared_context = self._make_context(
                    self._new_session(label=self._account_label(0), account=0))
            return self._shared_context
        if account not in self._account_contexts:
            self._account_contexts[account] = self._make_context(
                self._new_session(label=self._account_label(account), account=account))
        return self._account_contexts[account]

    def _account_label(self, account: int) -> str:
        return "main" if len(self.config.account_jars()) <= 1 else f"account-{account + 1}"

    # ---------------------------------------------------------- parallel run --

    def _plan_lanes(self, targets: list[Target]) -> list[Lane]:
        """One lane per account when cookies are supplied, else
        ``broConcurrency`` anonymous lanes; never more lanes than targets."""
        jars = self.config.account_jars()
        wanted = len(jars) if jars else self.config.bro.concurrency
        count = max(1, min(wanted, len(targets)))
        if jars:
            if count < len(jars):
                log.info("%d target(s) for %d accounts: account %s not used",
                         len(targets), len(jars),
                         ", ".join(str(n) for n in range(count + 1, len(jars) + 1)))
            return [Lane(label=self._account_label(i), account=i) for i in range(count)]
        return [Lane(label="main" if count == 1 else f"worker-{i + 1}") for i in range(count)]

    def _run_pool(self, targets: list[Target], lanes: list[Lane]) -> None:
        """Several browsers at once, each taking the next target from one queue.

        Each account keeps its own browser, exit IP, cookie injection and
        request pacing, so two accounts side by side add no load to either.
        A lane whose account or browser is lost stops at once (its VM is
        stopped) and hands its unfinished target back when that target had
        produced nothing yet. The run stops when no lane is left; the run
        budget (``maxApiRequests``, ``maxRunSeconds``) is shared and stops
        every lane.
        """
        accounts = lanes[0].account is not None
        log.info("running %d target(s) across %d browser session(s)%s",
                 len(targets), len(lanes), " (one per account)" if accounts else "")
        self.lanes = lanes
        queue = self._queue = TargetQueue(targets)
        threads = [threading.Thread(target=self._work_lane, args=(lane, queue),
                                    name=lane.label, daemon=True) for lane in lanes]
        for thread in threads:
            thread.start()
        for thread in threads:
            while thread.is_alive():
                thread.join(0.5)
        left = queue.drain()
        if isinstance(self._pool_stop, BudgetExceeded):
            self._pool_stop.partial_counted = True
            raise self._pool_stop
        if left:
            self.not_scraped = [target.input_url or target.url for _, target in left]
            reasons = "; ".join(f"{lane.label}: {lane.reason}" for lane in lanes if lane.reason)
            if not any(lane.status == "finished" for lane in lanes):
                raise FatalError(f"every browser session stopped, {len(left)} target(s) "
                                 f"not scraped ({reasons})")
            # Handed back after every other browser had run out of work and
            # stopped: idle browsers do not wait for hand-backs.
            for _, target in left:
                self.stats.record_failure(
                    target.input_url or target.url,
                    FatalError(f"handed back after the other browsers had stopped ({reasons})"))
            log.warning("%d target(s) handed back after the other browsers had stopped: %s",
                        len(left), ", ".join(self.not_scraped))
        for lane in lanes:
            if lane.status != "finished":
                log.warning("%s: %s (%s)", lane.label, lane.status, lane.reason)

    def _work_lane(self, lane: Lane, queue: TargetQueue) -> None:
        """One lane's thread: start its browser, then scrape until the queue
        is done, the lane is lost, or the run stops."""
        lane.status, lane.started_at = "running", time.time()
        scrape = None
        try:
            try:
                scrape = self._lane_context(lane)
            except BudgetExceeded as exc:
                self._stop_pool(exc)
                lane.status, lane.reason = "stopped", exc.reason
                return
            except Exception as exc:  # noqa: BLE001 - this lane's browser, route or cookies
                lane.status, lane.reason = "failed_to_start", _lane_reason(exc)
                log.error("%s could not start: %s", lane.label, lane.reason)
                return
            lane.ready_at = time.time()
            if lane.account is not None:
                lane.viewer_id = getattr(getattr(scrape.ctx, "tokens", None), "user_id", None)
            while True:
                item = queue.take()
                if item is None:
                    break
                target = item[1]
                try:
                    status, produced = self._scrape_target(scrape, target, lane=lane)
                except LaneLost as lost:
                    lane.records += lost.produced
                    budget = isinstance(lost.error, BudgetExceeded)
                    if queue.finish(item, hand_back=not budget and not lost.produced):
                        lane.targets_handed_back += 1
                        log.warning("%s stopped (%s); %s goes to another browser",
                                    lane.label, _lane_reason(lost.error), target)
                    else:
                        with self._lock:
                            if budget:
                                self.stats.targets_partial += 1
                                lane.targets_partial += 1
                            else:
                                self.stats.record_failure(target.input_url or target.url, lost.error)
                                lane.targets_failed += 1
                    if budget:
                        self._stop_pool(lost.error)
                    lane.status, lane.reason = "stopped", _lane_reason(lost.error)
                    return
                queue.finish(item)
                lane.records += produced
                if status == "done":
                    lane.targets_done += 1
                elif status == "partial":
                    lane.targets_partial += 1
                else:
                    lane.targets_failed += 1
            if self._pool_stop is not None:
                lane.status, lane.reason = "stopped", _lane_reason(self._pool_stop)
            else:
                lane.status = "finished"
        finally:
            lane.finished_at = time.time()
            if scrape is not None:
                lane.api_requests = int(getattr(scrape.ctx, "api_calls", 0) or 0)
                lane.ip_rotations = int(getattr(scrape.ctx, "rotations", 0) or 0)
                if getattr(lane, "stats", None) is not None:
                    with self._lock:
                        self.stats.absorb(lane.stats)
            session = getattr(lane, "session", None)
            if session is not None:
                # Stop this browser now: an idle VM bills, and a lost
                # account's cookies must not stay in a running browser.
                self._retire(session)

    def _lane_context(self, lane: Lane) -> ScrapeContext:
        """Start one lane's browser (or take the account browser that
        discovery already started) and give it its own counters."""
        if lane.account == 0 and self._shared_context is not None:
            scrape = self._shared_context
        else:
            session = self._new_session(label=lane.label, account=lane.account)
            lane.session = session
            scrape = self._make_context(session)
            if lane.account == 0:
                self._shared_context = scrape
            elif lane.account is not None:
                self._account_contexts[lane.account] = scrape
        lane.session = scrape.ctx.session
        lane.session_id = getattr(lane.session, "session_id", None)
        lane.stats = Stats()
        scrape.stats = lane.stats
        api = getattr(scrape, "api", None)
        if api is not None:
            api.pagination_log = lane.stats.pagination
        if lane.account is not None:
            scrape.seeds = self._seeds
        return scrape

    def _stop_pool(self, exc: BaseException) -> None:
        with self._lock:
            if self._pool_stop is None:
                self._pool_stop = exc
        if self._queue is not None:
            self._queue.close()

    # ------------------------------------------- complete mode, several accounts --

    def _sharded(self) -> bool:
        """Complete mode split over accounts: several accounts now, or a
        resumed checkpoint that was split before."""
        if self.config.collection_mode != "complete":
            return False
        if len(self.config.account_jars()) > 1:
            return True
        return bool(self.config.resume and isinstance(self.storage, PersistentStorage)
                    and self.storage.state.get_meta("shares"))

    def _run_shares(self, targets: list[Target]) -> None:
        """Complete mode with several accounts: one browser and one checkpoint
        (``accounts/<n>/``) per account, all at once; the run's dataset is the
        union of the checkpoints.

        Posts are handed out one at a time: every account's browser takes one
        post to start with, and the next post goes to whichever browser is
        free first, into the same browser and checkpoint. Posts of very
        different size therefore do not leave an account idle. The complete
        engine keeps its single-browser guarantees in each share: one account,
        one VM, one journal, cookies injected once. A share that stops keeps
        the post it was on in its checkpoint (the same account resumes it);
        the posts it had not taken go to the others. On resume every share
        first finishes its own posts, matched by the account id in the
        cookies, then takes new ones. The run budget (``maxApiRequests``,
        ``maxRunSeconds``) is shared; a share's error is raised once every
        share has ended and the dataset is merged.
        """
        state = self.storage.state
        jars = self.config.account_jars()
        plan = state.get_meta("shares")
        if not plan:
            if self.config.resume:
                raise InputError("this checkpoint was collected by one account: resume it "
                                 "with that account's cookies only")
            plan = share_plan.accounts_plan(jars)
            state.set_meta("shares", plan)
        accounts = share_plan.assign(plan, jars)
        planned = {entry.get("viewerId") for entry in plan}
        for index, jar in enumerate(jars):
            if cookie_viewer_id(jar) not in planned:
                log.warning("account %s is not part of this run's plan and is not used",
                            cookie_viewer_id(jar) or index + 1)
        root = Path(self.storage.root)
        # Plans saved before posts were handed out one at a time list each
        # share's posts; those shares keep them.
        fixed = {url for entry in plan for url in entry.get("directUrls") or []}
        taken = set().union(*(share_plan.taken_urls(root / e["directory"]) for e in plan))
        queue = collections.deque(t for t in targets if t.url not in taken
                                  and (t.input_url or t.url) not in fixed)
        queue_lock = threading.Lock()
        self.shares = [{"share": entry["share"], "label": f"account-{entry['share']}",
                        "viewerId": entry.get("viewerId"), "directory": entry["directory"],
                        "posts": [], "status": "pending"}
                       for entry in plan]
        errors: dict[int, BaseException] = {}
        runs: dict[int, ScraperRun] = {}

        def feed_for(report: dict[str, Any]):
            def feed() -> Target | None:
                with queue_lock:
                    target = queue.popleft() if queue else None
                if target is not None:
                    report["posts"].append(target.input_url or target.url)
                    log.info("%s takes %s (%d post(s) left)", report["label"], target, len(queue))
                return target
            return feed

        def work(entry: dict[str, Any], account: int, report: dict[str, Any]) -> None:
            directory = root / entry["directory"]
            resume = (directory / "state.sqlite").is_file()
            child = copy.deepcopy(self.config)
            child.direct_urls = list(entry.get("directUrls") or self.config.direct_urls)
            child.search = ""
            child.session_cookies, child.extra_accounts = list(jars[account]), []
            child.output_dir = directory
            child.resume = directory if resume else None
            child.materialize_items = False
            child.bro.concurrency = 1
            child.bro.skip_balance_check = True
            report.update(status="running", startedAt=time.time(), resumed=resume)
            storage = None
            try:
                # The share's checkpoint is opened on this thread: SQLite
                # connections belong to the thread that made them.
                storage = PersistentStorage(directory, fmt=child.output_format, resume=resume, name=child.dataset_name)
                run = runs[entry["share"]] = ScraperRun(child, storage=storage)
                run.budget = ShareBudget(self.budget)
                run.client.time_remaining = run.budget.remaining
                # a boot still in flight for another share is not this share's
                if hasattr(self.client, "creates"):
                    run.client.creates = self.client.creates
                if entry.get("directUrls"):
                    report["posts"] = list(entry["directUrls"])
                else:
                    run.feed = feed_for(report)
                run.run()
            except BaseException as exc:  # noqa: BLE001 - reported once every share has ended
                errors[entry["share"]] = exc
                report["error"] = _lane_reason(exc)
            finally:
                report["seconds"] = round(time.time() - report["startedAt"], 1)
                if storage is not None:
                    storage.dataset.stop_exporter()
                    storage.state.close()

        threads = []
        for entry, account, report in zip(plan, accounts, self.shares):
            if account is None:
                report.update(status="skipped", reason="account not supplied")
                log.warning("share %s (account %s) skipped: its cookies were not supplied",
                            entry["share"], entry.get("viewerId"))
                continue
            thread = threading.Thread(target=work, args=(entry, account, report),
                                      name=report["label"], daemon=True)
            threads.append(thread)
            thread.start()
        try:
            for thread in threads:
                while thread.is_alive():
                    thread.join(0.5)
        except BaseException:
            # Interrupted: stop every share's browser now rather than leave
            # it billing until getbro's idle limit ends it.
            for run in list(runs.values()):
                for session in list(run.sessions):
                    session.close()
            raise
        self.not_scraped = [t.input_url or t.url for t in queue]
        self._absorb_shares(targets)
        if errors:
            number, error = next(((n, e) for n, e in errors.items() if isinstance(e, FatalError)),
                                 next(iter(errors.items())))
            error.share_recorded = True
            raise error

    def _absorb_shares(self, targets: list[Target]) -> None:
        """Merge the shares' records into the run's checkpoint and take in
        their counters, bills and preflight reports."""
        root = Path(self.storage.root)
        order = {target.input_url: index for index, target in enumerate(targets)}
        share_plan.merge_records(self.storage.state,
                                 [(r["share"], root / r["directory"]) for r in self.shares], order)
        for report in self.shares:
            if report["status"] == "skipped":
                continue
            store = KeyValueStore(root / report["directory"] / "key_value_stores" / self.storage.name)
            output = store.directory / "OUTPUT.json"
            # A share that failed before its summary leaves the previous
            # invocation's OUTPUT in place; that one is not this run's.
            fresh = output.is_file() and output.stat().st_mtime >= report.get("startedAt", 0) - 1
            summary = (store.get("OUTPUT") or {}) if fresh else {}
            label = report["label"]
            self.stats.absorb(share_plan.stats_from_summary(summary.get("stats"), label=label))
            billing = summary.get("billing") or {}
            self._billing["total"] += float(billing.get("total") or 0.0)
            if billing.get("incomplete"):
                self._billing["incomplete"] = True
            for session in billing.get("sessions") or []:
                self._billing["sessions"].append({**session, "label": label})
            for preflight in summary.get("preflight") or []:
                self.preflight.append({**preflight, "label": label})
            coverage = summary.get("coverage") or {}
            metrics = summary.get("metrics") or {}
            report.update(
                status=("failed" if report.get("error") else summary.get("status") or "unknown"),
                itemCount=summary.get("itemCount"), stopReason=summary.get("stopReason"),
                traversal=coverage.get("traversal"), coverage=coverage or None,
                managedApiRequests=(summary.get("budget") or {}).get("managedApiRequests"),
                requestsPer100NewRecords=metrics.get("requestsPer100NewRecords"),
                billed=round(float(billing.get("total") or 0.0), 6))
            if self.stop_reason is None and summary.get("stopReason"):
                self.stop_reason = summary["stopReason"]

    def _finish_shares(self) -> RunResult:
        """The run's summary for a complete-mode run split over accounts."""
        self._billing["total"] = round(self._billing["total"], 6)
        count = len(self.storage.dataset)
        self.stats.items = count
        items = list(self.storage.dataset.records) if self.config.materialize_items else []
        dataset_path = str(self.storage.finish())
        coverage = share_plan.merge_coverage([r.get("coverage") for r in self.shares])
        shares = [{k: v for k, v in report.items() if k != "coverage"} for report in self.shares]
        partial = (self.stats.targets_failed or self.stats.targets_partial
                   or any(r.get("status") != "succeeded" for r in self.shares))
        summary_path = str(self.storage.save_summary({
            "resultsType": self.config.results_type.value,
            "itemCount": count,
            "status": "partial" if partial else "succeeded",
            "stats": self.stats.to_dict(),
            "billing": self._billing,
            "budget": self.budget.summary(),
            "preflight": self.preflight,
            "metrics": {"newUniqueRecords": count - self.starting_count,
                        "getbroTransport": "sdk", "getbroSdkVersion": SDK_VERSION},
            "stopReason": self.stop_reason,
            "coverage": coverage,
            "collectionPhase": self.config.collection_phase,
            "accounts": len(self.config.account_jars()),
            "shares": shares,
            "targetsNotScraped": list(self.not_scraped),
        }))
        log.info("done: %d item(s) from %d share(s), %d target(s) ok, %d failed, $%.4f billed",
                 count, len(self.shares), self.stats.targets_done, self.stats.targets_failed,
                 self._billing["total"])
        return RunResult(items=items, stats=self.stats, billing=self._billing,
                         dataset_path=dataset_path, summary_path=summary_path, lanes=shares)

    def _scrape_target(self, scrape: ScrapeContext, target: Target, *,
                       lane: Lane | None = None) -> tuple[str, int]:
        """Scrape one target; returns ``(status, records)`` with status
        ``done``, ``partial`` or ``failed``.

        In a parallel run (``lane`` given) an error that ends the lane -- the
        account is refused, throttled or challenged, the browser is gone, the
        budget is spent -- is raised as :class:`LaneLost`; alone, a fatal error
        propagates and a browser error is recorded against the target.
        """
        log.info("scraping %s (%s)", target.url, self.config.results_type.value)
        scraper = scraper_for(self.config.results_type, scrape)
        produced = 0
        pagination = (getattr(scrape, "stats", None) or self.stats).pagination
        pagination_start = len(pagination)
        try:
            target = resolve_user_id(target, scrape.api)
            scrape.remember_user_id(target.key, (target.extra or {}).get("user_id"))
            search_term = target.extra.get("search_term")
            for record in scraper.run(target):
                if search_term:
                    # Apify's records for search-found targets name the query.
                    record.setdefault("searchTerm", search_term)
                    record.setdefault("searchSource", target.extra.get("search_source"))
                self._emit(record)
                produced += 1
            partial = any(p.get("issues") for p in pagination[pagination_start:])
            with self._lock:
                if partial:
                    self.stats.targets_partial += 1
                else:
                    self.stats.targets_done += 1
            log.info("%s -> %d record(s)", target, produced)
            return ("partial" if partial else "done"), produced
        except FatalError as exc:
            if lane is None:
                raise
            raise LaneLost(exc, produced) from exc
        except (BroSessionError, BroTimeoutError) as exc:
            if lane is not None:
                raise LaneLost(exc, produced) from exc
            log.error("%s failed: %s: %s", target, type(exc).__name__, exc)
            with self._lock:
                self.stats.record_failure(target.input_url or target.url, exc)
        except (ScraperError, InstagramError) as exc:
            log.error("%s failed: %s: %s", target, type(exc).__name__, exc)
            with self._lock:
                self.stats.record_failure(target.input_url or target.url, exc)
        except Exception as exc:  # noqa: BLE001 - never lose the rest of the run
            log.exception("%s crashed", target)
            with self._lock:
                self.stats.record_failure(target.input_url or target.url, exc)
        return "failed", produced

    def _emit(self, record: dict[str, Any]) -> None:
        with self._lock:
            if self.storage:
                self.storage.dataset.push(record)
            else:
                self._collected.append(record)

    # ------------------------------------------------------------- sessions --

    def _new_session(self, *, label: str, account: int | None = 0) -> BroSession:
        """A browser for account ``account`` (an index into
        ``ScraperInput.account_jars()``) or, with ``None``, an anonymous one.
        The cookies go in later, once, after the anonymous route check."""
        session = BroSession(
            self.client,
            session_kwargs=self.config.proxy.to_session_kwargs(),
            session_timeout=self.config.bro.session_timeout,
            command_timeout=self.config.bro.command_timeout,
            recovery_timeout=self.config.bro.recovery_timeout,
            cookies=[],
            label=label,
        )
        session.account_index = account
        session.no_restart = bool(self.config.session_cookies) or self.config.collection_mode == "complete"
        session.time_remaining = self.budget.remaining
        if self.config.collection_mode == "complete" and self.config.resume:
            saved = self.storage.state.get_meta("sessionId")
            if saved:
                session.attach(saved)
                if not session.session_id and self.storage.state.get_meta("viewerId") and not self.config.session_cookies:
                    raise InputError("The saved browser has stopped; supply --cookies-file for an explicit new-session resume")
                if not session.session_id and self.config.session_cookies:
                    # Every command of the stopped VM is beyond reach now, not
                    # only the latest one; each is deferred, none is replayed.
                    for _ in range(64):
                        operation = self.storage.state.unfinished_operation()
                        if not operation:
                            break
                        self.storage.state.update_operation(
                            operation["id"], "deferred", phase="old_vm_stopped",
                            last_status=operation.get("last_status"),
                            error_type="PreviousVmUnavailable")
        with self._lock:
            self.sessions.append(session)
        return session

    def _resolve_profiles_anonymously(self, ctx: IgContext, report: dict[str, Any]) -> dict[str, dict[str, Any]]:
        """Resolve profile IDs before cookie injection where possible.

        Measured 2026-09-22 (auth-probe): with cookies that endpoint answered
        HTTP 429 for this account, while an anonymous read succeeded. A later
        anonymous read also returned a login wall; in that case the rendered
        profile page is one further safe source of the numeric ID. The
        id-keyed reads the tagged feed, stories and search actually need
        (``users/{id}/info``, ``usertags/{id}/feed``, ``reels_media``,
        ``topsearch``) all answered 200. Try the anonymous API once per
        distinct unfinished username; if it rejects the read, inspect that
        profile's public page before cookies. Endpoint errors are logged;
        getbro transport and fatal errors still stop before cookies are passed.
        """
        wanted: dict[str, str] = {}
        for target in self._targets:
            if target.type in (TargetType.PROFILE, TargetType.STORY, TargetType.TAGGED_FEED,
                               TargetType.REELS_FEED) and not (target.extra or {}).get("user_id"):
                if isinstance(self.storage, PersistentStorage):
                    saved = self.storage.state.db.execute(
                        "SELECT status FROM targets WHERE id=?", (target.url,)).fetchone()
                    if saved and saved[0] == "done":
                        continue
                wanted.setdefault(target.key.lower(), target.key)
        if not wanted:
            return {}
        api = InstagramApi(ctx, pause=self.config.media_lookup_pause)
        seeded: dict[str, dict[str, Any]] = {}
        outcome: dict[str, str] = {}
        sources: dict[str, str] = {}
        for key, username in wanted.items():
            if not self._seeds.claim(key):
                continue  # another browser of this run reads (or has read) it
            record = None
            try:
                record = self._read_profile_anonymously(api, ctx, username, key, outcome, sources)
            finally:
                self._seeds.settle(key, record, sources.get(key))
            if record:
                seeded[key] = record
                if isinstance(self.storage, PersistentStorage):
                    self.storage.state.cache_set(
                        f"profile-id:{key}", {"id": str(record["id"]),
                                              "username": str(record.get("username") or username).lower(),
                                              "observedAt": time.time()})
        report["anonymousProfileIds"] = outcome
        report["anonymousProfileIdSources"] = sources
        return seeded

    def _read_profile_anonymously(self, api: InstagramApi, ctx: IgContext, username: str, key: str,
                                  outcome: dict[str, str], sources: dict[str, str]) -> dict[str, Any] | None:
        """One username: the anonymous profile API, then its public page."""
        try:
            record = api.profile(username)
        except BudgetExceeded:
            raise
        except InstagramError as exc:
            outcome[username] = type(exc).__name__
            log.info("anonymous profile API read for @%s failed (%s)",
                     username, type(exc).__name__)
            if isinstance(exc, NotFoundError) or not hasattr(ctx, "goto"):
                return None
            try:
                landed = ctx.goto(ig_ep.profile_page(username), wait=4)
                path = urlsplit(str(landed or ctx.current_page)).path.rstrip("/")
                if path.lower() != f"/{username.lower()}":
                    outcome[username] += ";page_redirected"
                    return None
                user_id = ig_page.read_user_id(ctx, username, navigate=False)
            except InstagramError as page_exc:
                outcome[username] += ";" + type(page_exc).__name__
                return None
            if not user_id:
                outcome[username] += ";page_missing"
                return None
            record = {"id": str(user_id), "username": username}
            sources[key] = "profile_page"
        if not record.get("id"):
            return None
        outcome[username] = str(record["id"])
        sources.setdefault(key, "web_profile_info")
        return record

    def _make_context(self, session: BroSession) -> ScrapeContext:
        # Each browser carries the cookies of its own account only (see
        # ``_new_session``); ``None`` marks an anonymous browser.
        jars = self.config.account_jars()
        account = getattr(session, "account_index", 0)
        if account is not None and not isinstance(account, int):
            account = 0  # a browser made elsewhere carries the first account, as before
        jar = list(jars[account]) if account is not None and 0 <= account < len(jars) else []
        ctx = IgContext(
            session=session,
            cookies=[],
            request_pause=self.config.authenticated_request_pause,
            budget=self.budget,
            read_retries=self.config.max_request_retries,
            strict_auth=True,
            traffic=self.traffic,
            resume_saved_login=self.config.resume_saved_login,
            read_timeout=self.config.bro.read_timeout,
        )
        attached = session.session_id is not None
        if not attached:
            session.start()
        if self.config.collection_mode == "complete":
            self.storage.state.set_meta("sessionId", session.session_id)
        report = {"sessionId": session.session_id, "attached": attached}
        if len(jars) > 1 or self.config.bro.concurrency > 1:
            report["label"] = getattr(session, "label", None)
        with self._lock:
            self.preflight.append(report)
        seeded: dict[str, dict[str, Any]] = {}
        seed_sources: dict[str, str] = {}
        restored_ids: dict[str, str] = {}
        if attached:
            # An explicit resume can keep the still-alive browser and its jar.
            ctx.cookies = list(jar)
            session.cookies = list(jar)
            ctx.current_page = session.current_url()
            ctx.tokens = ctx.read_tokens()
            ctx.bootstrapped = True
            ctx.authenticated_since = time.time()
            report.update(cookiesInjected=False, authentication="passed" if ctx.authenticated else "failed")
            if isinstance(self.storage, PersistentStorage):
                for target in self._targets:
                    if target.type in (TargetType.PROFILE, TargetType.STORY,
                                       TargetType.TAGGED_FEED, TargetType.REELS_FEED):
                        saved_id = self.storage.state.cache_get(
                            f"profile-id:{target.key.lower()}") or {}
                        if saved_id.get("username") == target.key.lower() and saved_id.get("id"):
                            restored_ids[target.key.lower()] = str(saved_id["id"])
        else:
            verify_route(ctx, report)
            if jar:
                if self.config.anonymous_profile_ids:
                    self._resolve_profiles_anonymously(ctx, report)
                # this browser's reads and every other browser's finished ones
                seeded, seed_sources = self._seeds.snapshot()
                ctx.authenticated_since = time.time()
                ctx.cookies = list(jar)
                session.cookies = list(jar)
                report["cookiesInjectionAttempted"] = True
                ctx.bootstrap(force=True)
                report["cookiesInjected"] = True
                report["authentication"] = "passed" if ctx.authenticated else "failed"
                report["webConfirmed"] = bool(ctx.tokens.web_confirmed)
                if not ctx.tokens.web_confirmed:
                    # where the cookies landed: the web app, or something else
                    report["landingPath"] = ctx.tokens.page_path
                if ctx.tokens.logged_out_reason:
                    report["authenticationDetail"] = ctx.tokens.logged_out_reason
                if ctx.saved_login is not None:
                    report["savedLoginContinue"] = dict(ctx.saved_login)
        ctx.account_bound = ctx.authenticated
        expected_viewer = (self.storage.state.get_meta("viewerId")
                           if self.config.collection_mode == "complete" else None)
        if expected_viewer and not ctx.authenticated:
            raise FatalError("The resumed browser no longer has authenticated access")
        if self.config.collection_mode == "complete" and ctx.authenticated:
            previous_viewer = self.storage.state.get_meta("viewerId")
            viewer = ctx.tokens.user_id
            if previous_viewer and previous_viewer != viewer:
                raise FatalError("Resume viewer differs from checkpoint account")
            self.storage.state.set_meta("viewerId", viewer)

        # Supplying cookies is an explicit request for an authenticated run.
        # If Instagram rejects them, degrading to the vision fallback is not a
        # kindness: one measured run burned 724s and $0.26 to produce 33
        # caption-less records when the real answer was "your sessionid is
        # dead". Say so instead, unless the caller opted into degrading.
        if (jar
                and not ctx.authenticated
                and (self.config.require_valid_session or self.config.collection_mode == "complete")):
            landing = ""
            if ctx.tokens.password_prompt and ctx.saved_login and ctx.saved_login.get("clicked"):
                landing = ("After 'Continue' on the saved-account screen Instagram asked for "
                           "the password; the scraper never enters one -- log in in your "
                           "browser and export fresh cookies. ")
            elif ctx.tokens.logged_out_reason == "logged_out_landing":
                landing = ("Instagram shows the saved-account 'Continue' screen for these "
                           "cookies" + (" and pressing it did not log in"
                                        if ctx.saved_login and ctx.saved_login.get("clicked") else "")
                           + " -- export fresh cookies from a browser where Instagram is open "
                           "and logged in. ")
            raise FatalError(
                landing +
                "sessionCookies were supplied but Instagram served the "
                "logged-out page, so this run would silently fall back to "
                "slow, thin, AI-scraped results. Supply a fresh sessionid "
                "(and make sure the proxy country matches where that session "
                "was created), or set `requireValidSession: false` to accept "
                "the degraded output."
            )

        known_ids = {target.key.lower(): str((target.extra or {})["user_id"])
                     for target in self._targets if (target.extra or {}).get("user_id")}
        return ScrapeContext(
            ctx=ctx,
            api=InstagramApi(ctx, pause=self.config.media_lookup_pause,
                             pagination_log=self.stats.pagination,
                             max_comment_pages=self.config.max_comment_pages),
            ai=AiExtractor(ctx, model_size=self.config.ai_model_size,
                           enabled=self.config.ai_fallback),
            config=self.config,
            stats=self.stats,
            _user_ids={**{k: str(v.get("id")) for k, v in seeded.items()},
                       **restored_ids, **known_ids},
            _profiles={k: ScrapeContext.viewer_independent_profile(v)
                       for k, v in seeded.items()
                       if seed_sources.get(k) != "profile_page"},
            _anonymous_profiles=({k for k in seeded
                                  if seed_sources.get(k) != "profile_page"}
                                 if not attached else set()),
        )

    def _recover_checkpoint_command(self, session: BroSession, *, ctx=None) -> None:
        """Resolve a command left by the prior invocation before browser work."""
        if not isinstance(self.storage, PersistentStorage):
            return
        state = self.storage.state
        operation = state.unfinished_operation()
        if not operation or state.pending_page(operation.get("job_id")):
            return
        command_id = operation.get("command_id")
        if not command_id:
            state.update_operation(operation["id"], "outcome_unknown",
                                   session_id=session.session_id, phase="resume_submit",
                                   last_status="unknown",
                                   error_type="SubmitOutcomeUnknown")
            raise BroCommandOutcomeUnknown(
                "checkpoint contains an operation with an unknown submit outcome",
                session_id=session.session_id, phase="resume_submit",
                last_status="unknown")
        if operation.get("session_id") and operation["session_id"] != session.session_id:
            raise BroCommandOutcomeUnknown(
                "unfinished command belongs to a different browser session",
                session_id=operation.get("session_id"), command_id=command_id,
                phase="resume", last_status=operation.get("last_status"))
        result = self.client.await_command(
            session.session_id, command_id, timeout=0,
            recovery_timeout=self.config.bro.recovery_timeout)
        status = str(result.get("status") or "")
        steps = untag_steps(list((result.get("response") or {}).get("commands") or []))
        if status != "done" and operation.get("operation_kind") != "page_fetch":
            failed = next((step for step in steps if not step.get("success")), {})
            state.update_operation(operation["id"], "failed", phase="resume",
                                   last_status=status, error_type=str(
                                       failed.get("error_name") or result.get("error_name") or "CommandFailed"))
            if operation.get("operation_kind") == "page_fetch_poll":
                # This read-only observation failed, not the Instagram fetch.
                # Its known terminal outcome permits reading the original slot.
                return self._recover_checkpoint_command(session, ctx=ctx)
            job = state.db.execute("SELECT * FROM jobs WHERE id=?", (operation.get("job_id"),)).fetchone()
            if job:
                payload = json.loads(job["payload"])
                payload.update(stopReason="BroCommandError",
                               commandError=str(failed.get("error_name") or result.get("error_name") or "CommandFailed"))
                state.db.execute("UPDATE jobs SET payload=?,status='blocked' WHERE id=?",
                                 (json.dumps(payload, ensure_ascii=False, separators=(",", ":")), job["id"]))
            return
        try:
            payload = step_data(steps[-1]) if steps else None
        except BroPayloadError:
            state.update_operation(operation["id"], "payload_error", phase="payload_download",
                                   last_status="done", error_type="BroPayloadError")
            raise
        if operation.get("operation_kind") in {
                "native_ui", "parent_scroll", "parent_settle", "reply_click", "reply_wait",
                "thread_list", "scan_step", "scan_to_parent", "anchor_restore",
                "parent_dom_check", "gap_audit_inspect", "gap_audit_focus", "observer_install",
                "observer_read", "observer_ack", "ui_phase",
                "witness_install", "witness_read"}:
            # The command result only confirms the UI action. The page itself
            # is recovered from the browser (an unacknowledged observer read
            # is simply read again) by the collector in the same attached VM,
            # or re-observed after the next explicit invocation.
            phase = ("resume_har" if operation.get("operation_kind") in
                     {"native_ui", "parent_scroll", "reply_click", "observer_read"} else "resume_ui_result")
            state.update_operation(operation["id"], "deferred", phase=phase,
                                   last_status="done",
                                   error_type="NativeUiPageRequiresRecapture")
            return
        result_body = payload.get("result") if isinstance(payload, dict) else None
        is_poll = operation.get("operation_kind") == "page_fetch_poll"
        is_inline = operation.get("operation_kind") == "page_fetch"
        is_submit = isinstance(result_body, str) and result_body in ("submitted", "exists")
        if is_poll or is_submit or is_inline:
            # A page-context GET: the command only submitted it. Its result
            # lives in the page under the identity derived from the journaled
            # request fingerprint -- read it there, never fetch again.
            initial_slot = None
            if is_inline:
                try:
                    candidate = json.loads(result_body) if isinstance(result_body, str) else None
                    if isinstance(candidate, dict) and candidate.get("state") in ("pending", "done", "error"):
                        initial_slot = candidate
                except (TypeError, ValueError):
                    pass
            if is_poll:
                try:
                    initial_slot = json.loads(result_body) if isinstance(result_body, str) else None
                except ValueError:
                    initial_slot = None
                poll_operation = operation
                parent = state.db.execute("""SELECT * FROM command_operations
                    WHERE job_id=? AND request_fingerprint=? AND attempt=?
                    AND operation_kind='page_fetch' AND id<>?
                    ORDER BY created_at DESC LIMIT 1""", (
                        operation["job_id"], operation["request_fingerprint"],
                        operation["attempt"], operation["id"])).fetchone()
                if not parent:
                    raise BroCommandOutcomeUnknown(
                        "page-fetch observation has no matching submit operation",
                        session_id=session.session_id, command_id=command_id,
                        phase="page_fetch_recovery", last_status="done")
                state.update_operation(poll_operation["id"], "applied",
                                       phase="resume_page_fetch_observation", last_status="done")
                operation = dict(parent)
            key = page_fetch_key(operation.get("request_fingerprint") or "", operation.get("attempt", 1))
            try:
                slot = wait_page_fetch_slot(
                    session, key, operation_store=state, parent_operation=operation,
                    time_remaining=self.budget.remaining, initial_slot=initial_slot,
                    missing_ok=True, wait=self.config.bro.read_timeout)
            except BroCommandOutcomeUnknown as exc:
                if not getattr(exc, "page_fetch_poll_failure", False):
                    state.update_operation(operation["id"], "outcome_unknown",
                                           phase="page_fetch", last_status="pending")
                raise
            if slot is None:
                state.update_operation(operation["id"], "deferred", phase="page_fetch_slot_unavailable",
                                       last_status="done", error_type="PageFetchSlotUnavailable")
                return
            state.set_meta("recoveredPageFetches", state.get_meta("recoveredPageFetches", 0) + 1)
            payload = slot
            result_body = payload.get("result")
        # Recovery must enforce the same HTTP/authentication classification as
        # a normal read before treating any JSON object as a paid data page.
        classifier = ctx or IgContext(session=session, strict_auth=True)
        try:
            decoded = classifier._unwrap(payload, url="recovered read", what="recovered comment page")
        except (RateLimitedError, ChallengeRequiredError, LoginRequiredError) as exc:
            state.update_operation(operation["id"], "failed", phase="decode", last_status="done",
                                   error_type=type(exc).__name__)
            raise FatalError(str(exc)) from exc
        except PageFetchReadTimeoutError as exc:
            state.update_operation(operation["id"], "failed", phase="page_fetch_cancelled",
                                   last_status="done", error_type=type(exc).__name__)
            row = state.db.execute("SELECT * FROM jobs WHERE id=?", (operation.get("job_id"),)).fetchone()
            if row:
                job = dict(row)
                job["payload"] = json.loads(job["payload"])
                job["payload"]["stopReason"] = "PageFetchReadTimeoutError"
                state.save_job(job, status="blocked")
            state.add_invocation_metric("pageFetchReadTimeouts", 1)
            return
        except InstagramError as exc:
            state.update_operation(operation["id"], "failed", phase="decode", last_status="done",
                                   error_type=type(exc).__name__)
            raise
        if not isinstance(decoded, (dict, list)):
            state.update_operation(operation["id"], "failed", phase="decode",
                                   last_status="done", error_type="RecoveredPayloadInvalid")
            raise BroCommandError(
                "RecoveredPayloadInvalid", "completed command did not contain JSON", "fetch_json",
                session_id=session.session_id, command_id=command_id,
                phase="decode", last_status="done")
        source = "api"
        job = state.db.execute("SELECT kind,payload FROM jobs WHERE id=?",
                               (operation.get("job_id"),)).fetchone()
        if job:
            job_payload = json.loads(job["payload"])
            if job_payload.get("source") == "native" and job_payload.get("template"):
                decoded = native_comments.normalize(
                    decoded, job_payload["template"], replies=job["kind"] == "replies")
                source = "native"
        state.save_pending_page(operation["job_id"], operation["id"], source, decoded)
        state.set_meta("recoveredCommands", state.get_meta("recoveredCommands", 0) + 1)
        state.set_meta("recoveredCommandsThisInvocation",
                       state.get_meta("recoveredCommandsThisInvocation", 0) + 1)

    def _retire(self, session: BroSession) -> None:
        """Record a session's billing and stop it."""
        try:
            try:
                # The closing accounting dump belongs to the HAR source. With
                # the observer the statistics are partial by declaration and a
                # a late cumulative export can outlast cleanup's small budget.
                if (session.is_open and self.stop_reason != "command_outcome_unknown"
                        and not self.traffic.partial):
                    self.traffic.observe(session.run_one(
                        cmd.dump_har_logs(resource_types=self.traffic.resource_types),
                        retries=0, timeout=15))
            except Exception:
                pass  # telemetry must never interfere with stopping a VM
            with self._cleanup_window():  # cleanup must run after the collection deadline
                billing = session.stop_and_sync()
            total = float(billing.get("total_billed") or 0.0)
            synced = bool(billing.get("billingSynced", True))
            with self._lock:
                self._billing["total"] += total
                if not synced:
                    # The VM is stopped but getbro never showed its bill:
                    # the total is a floor, not the cost of the run.
                    self._billing["incomplete"] = True
                self._billing["sessions"].append({
                    "sessionId": session.session_id,
                    "label": session.label,
                    "restarts": session.restarts,
                    "totalBilled": round(total, 6),
                    "breakdown": billing.get("breakdown", {}),
                    "confirmedStopped": billing.get("confirmedStopped", False),
                    "billingSynced": synced,
                })
        except Exception as exc:  # noqa: BLE001 - billing is best-effort
            log.debug("could not read billing for %s: %s", session.label, exc)
            with self._lock:
                self._billing["sessions"].append({"sessionId": session.session_id, "label": session.label,
                                                  "confirmedStopped": False, "billingError": type(exc).__name__})
        finally:
            session.close()
            with self._lock:
                if session in self.sessions:
                    self.sessions.remove(session)

    @contextmanager
    def _cleanup_window(self):
        """Lift the run deadline from the getbro client while a VM is stopped.

        Browsers of a parallel run stop at different times while others still
        work; the deadline comes back only when the last stop in progress ends.
        """
        with self._lock:
            if not self._cleanups:
                self._saved_time_remaining = self.client.time_remaining
                self.client.time_remaining = None
            self._cleanups += 1
        try:
            yield
        finally:
            with self._lock:
                self._cleanups -= 1
                if not self._cleanups:
                    self.client.time_remaining = self._saved_time_remaining

    def _shutdown(self) -> None:
        for session in list(self.sessions):
            self._retire(session)

    def _flush_checkpoint(self) -> None:
        if not self.storage:
            return
        self.storage.dataset.flush()
        if isinstance(self.storage, PersistentStorage):
            self.storage.state.set_meta("checkpointSavedAt", time.time())

    # --------------------------------------------------------------- finish --

    def _finish(self) -> RunResult:
        if self.shares:
            return self._finish_shares()
        self._billing["total"] = round(self._billing["total"], 6)
        count = len(self.storage.dataset) if self.storage else len(self._collected)
        items = ((list(self.storage.dataset.records) if self.storage else list(self._collected))
                 if self.config.materialize_items else [])
        self.stats.items = count
        coverage = self.storage.state.coverage() if isinstance(self.storage, PersistentStorage) else None
        new_records = count - self.starting_count
        first_result = self.storage.state.get_meta("firstResultThisInvocation") if coverage is not None else None
        metrics = {"newUniqueRecords": new_records,
                   "requestsPer100NewRecords": round(self.budget.requests * 100 / new_records, 3) if new_records > 0 else None,
                   "secondsToFirstNewRecord": round(first_result - self.started_at, 3) if first_result else None,
                   "getbroTransport": "sdk",
                   "getbroSdkVersion": SDK_VERSION,
                   "getbroReadTimeout": self.config.bro.read_timeout,
                   "getbroManagementRequests": self.client.management_requests,
                   "stalledDispatches": getattr(self.client, "stalled_dispatches", 0),
                   "lostSubmitsAdopted": getattr(self.client, "adopted_submits", 0),
                   "lostSubmitsNotAccepted": getattr(self.client, "rejected_submits", 0),
                   "resentSubmits": getattr(self.client, "resent_submits", 0),
                   "lostSubmitsAmbiguous": getattr(self.client, "ambiguous_submits", 0),
                   "duplicateSubmits": getattr(self.client, "duplicate_submits", 0),
                   "abandonedCreates": getattr(self.client, "abandoned_creates", 0),
                   "unclaimedBoots": getattr(self.client, "unclaimed_boots", 0),
                   "recoverySeconds": round(self.client.recovery_seconds, 3),
                   "recoveredCommands": self.client.recovered_commands,
                   "unknownCommands": self.client.unknown_commands}
        metrics["observationFaultsInjected"] = self.client.observation_faults_injected
        if coverage is not None:
            first_failure_count = self.storage.state.get_meta("recordsAtFirstCommandFailure")
            metrics.update(
                recordsAfterFirstFailure=(count - first_failure_count
                                          if isinstance(first_failure_count, int) else 0),
                pageLoads=self.storage.state.get_meta("pageLoads", 0),
                repeatedPageLoads=self.storage.state.get_meta("repeatedPageLoads", 0),
                recoveredCommands=self.storage.state.get_meta(
                    "recoveredCommandsThisInvocation", 0),
                recoveredCommandsTotal=self.storage.state.get_meta("recoveredCommands", 0),
            )
        dataset_path = summary_path = None

        if self.storage:
            dataset_path = str(self.storage.finish())
            if self.search_hits:
                self.storage.kv.set("SEARCH_HITS", self.search_hits)
            summary_path = str(self.storage.save_summary({
                "resultsType": self.config.results_type.value,
                "itemCount": count,
                "status": "partial" if (self.stats.targets_failed or self.stats.targets_partial)
                          else "succeeded",
                "stats": self.stats.to_dict(),
                "searchHits": len(self.search_hits),
                "unsupportedInputs": list(self.unsupported_inputs),
                "billing": self._billing,
                "budget": self.budget.summary(), "preflight": self.preflight,
                "traffic": self.traffic.summary(),
                "metrics": metrics,
                "commandRecovery": (self.storage.state.command_summary()
                                    if coverage is not None else None),
                "deepCollection": (self.storage.state.deep_metrics()
                                   if coverage is not None else None),
                "responseSource": ({"configured": self.config.response_source,
                                    "effective": ("har_filtered" if self.storage.state.get_meta("responseSourceFallback")
                                                  else self.config.response_source),
                                    "fallback": self.storage.state.get_meta("responseSourceFallback"),
                                    "observer": self.storage.state.get_meta("observerSummary"),
                                    "verification": self.storage.state.get_meta("observerVerification"),
                                    "unavailable": self.storage.state.get_meta("observerUnavailable"),
                                    "dropped": self.storage.state.get_meta("observerDropped", 0),
                                    "harResourceTypes": self.har_resource_types,
                                    "harDumpLog": self.storage.state.get_meta("harDumpLog"),
                                    "harWitness": self.storage.state.get_meta("harWitnessSummary"),
                                    "harWitnessUnavailable": self.storage.state.get_meta("harWitnessUnavailable"),
                                    "harObserverComparison": self.storage.state.get_meta("harObserverComparison")}
                                   if coverage is not None else None),
                "checkpointSaved": bool(self.storage.state.get_meta("checkpointSavedAt"))
                                   if coverage is not None else None,
                "stopReason": self.stop_reason, "coverage": coverage,
                "collectionPhase": self.config.collection_phase,
                "enrichmentStopReason": (self.storage.state.get_meta("enrichmentStopReason")
                                          if coverage is not None else None),
                "nativeValidation": ({"parents": self.storage.state.get_meta("nativeParentsValidated", False),
                                       "replies": self.storage.state.get_meta("nativeRepliesValidated", False)} if coverage is not None else None),
                **({"lanes": [lane.report() for lane in self.lanes],
                    "targetsNotScraped": list(self.not_scraped)} if self.lanes else {}),
            }))

        log.info("done: %d item(s), %d target(s) ok, %d failed, $%.4f billed",
                 count, self.stats.targets_done, self.stats.targets_failed,
                 self._billing["total"])
        return RunResult(
            items=items,
            stats=self.stats,
            billing=self._billing,
            dataset_path=dataset_path,
            summary_path=summary_path,
            search_hits=list(self.search_hits),
            lanes=[lane.report() for lane in self.lanes],
        )


def run_scraper(config: ScraperInput, *, storage: RunStorage | None = None) -> RunResult:
    """Convenience wrapper around :class:`ScraperRun`."""
    return ScraperRun(config, storage=storage).run()


def _lane_reason(exc: BaseException) -> str:
    """What stopped a browser, for the run summary (never page bodies)."""
    if isinstance(exc, BudgetExceeded):
        return exc.reason
    if isinstance(exc, LaneLost):
        exc = exc.error
    return f"{type(exc).__name__}: {str(exc)[:200]}"
