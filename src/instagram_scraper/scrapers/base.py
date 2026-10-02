"""Shared plumbing for the per-`resultsType` scrapers.

Each scraper receives a :class:`ScrapeContext` (browser, API client, AI
fallback, validated input, run statistics) and yields finished records for one
:class:`~instagram_scraper.urls.Target`.
"""

from __future__ import annotations

import logging
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Iterator

from ..errors import (
    ChallengeRequiredError,
    ExtractionError,
    InstagramError,
    InstagramServerError,
    LoginRequiredError,
    NotFoundError,
    PrivateProfileError,
    RateLimitedError,
)
from ..ig import page
from ..ig.ai import AiExtractor
from ..ig.api import InstagramApi
from ..ig.context import IgContext
from ..input_model import ScraperInput
from ..urls import Target

log = logging.getLogger(__name__)


@dataclass
class Stats:
    """Counters surfaced in the run summary."""

    items: int = 0
    targets_done: int = 0
    targets_failed: int = 0
    targets_partial: int = 0
    api_calls: int = 0
    ai_calls: int = 0
    #: comments skipped by `onlyPostsNewerThan` / `untilDate` (standard scraper)
    date_filtered: int = 0
    fallbacks: Counter = field(default_factory=Counter)
    errors: Counter = field(default_factory=Counter)
    failures: list[dict[str, str]] = field(default_factory=list)
    pagination: list[dict[str, Any]] = field(default_factory=list)

    def record_failure(self, target: str, error: Exception) -> None:
        self.targets_failed += 1
        self.errors[type(error).__name__] += 1
        self.failures.append({
            "target": target,
            "error": type(error).__name__,
            "message": str(error)[:500],
        })

    def absorb(self, other: "Stats") -> None:
        """Add one browser's counters to the run's (each browser of a
        parallel run counts into its own ``Stats``, so what a target
        reported is not mixed with another browser's target)."""
        self.items += other.items
        self.targets_done += other.targets_done
        self.targets_failed += other.targets_failed
        self.targets_partial += other.targets_partial
        self.api_calls += other.api_calls
        self.ai_calls += other.ai_calls
        self.date_filtered += other.date_filtered
        self.fallbacks.update(other.fallbacks)
        self.errors.update(other.errors)
        self.failures.extend(other.failures)
        self.pagination.extend(other.pagination)

    def to_dict(self) -> dict[str, Any]:
        return {
            "items": self.items,
            "targetsSucceeded": self.targets_done,
            "targetsFailed": self.targets_failed,
            "targetsPartial": self.targets_partial,
            "aiFallbackCalls": self.ai_calls,
            "dateFilteredComments": self.date_filtered,
            "fallbacksUsed": dict(self.fallbacks),
            "errors": dict(self.errors),
            "failures": self.failures,
            "commentPagination": self.pagination,
        }


@dataclass
class ScrapeContext:
    """Everything a scraper needs, assembled once per browser session."""

    ctx: IgContext
    api: InstagramApi
    ai: AiExtractor
    config: ScraperInput
    stats: Stats = field(default_factory=Stats)
    #: username -> numeric id, so repeated targets cost one request
    _user_ids: dict[str, str] = field(default_factory=dict)
    _profiles: dict[str, dict[str, Any]] = field(default_factory=dict)
    #: Profile snapshots read before cookie injection are not viewer-specific.
    _anonymous_profiles: set[str] = field(default_factory=set)
    #: A parallel run's anonymous profile reads (:class:`~instagram_scraper.pool.ProfileSeeds`),
    #: shared by its account browsers; None in a single-browser run.
    seeds: Any = None

    #: seconds to wait for a profile another browser is reading anonymously
    SEED_WAIT = 90.0

    @staticmethod
    def viewer_independent_profile(raw: dict[str, Any]) -> dict[str, Any]:
        """Retain public fields from an anonymous snapshot, never viewer state."""
        return {field: value for field, value in raw.items()
                if "viewer" not in field and field != "friendship_status"}

    def _adopt_seed(self, key: str) -> None:
        """Take a profile another browser of the run read anonymously."""
        if self.seeds is None or (key in self._user_ids and key in self._profiles):
            return
        found = self.seeds.get(key, wait=self.SEED_WAIT)
        if not found:
            return
        record, source = found
        if not self._user_ids.get(key):
            self._user_ids[key] = str(record["id"])
        if source != "profile_page" and key not in self._profiles:
            self._profiles[key] = self.viewer_independent_profile(record)
            self._anonymous_profiles.add(key)

    def remember_user_id(self, username: str, user_id: str | int | None) -> None:
        """Reuse a verified target/search ID without a username endpoint read."""
        if user_id is not None and str(user_id).isdigit():
            self._user_ids[username.lower()] = str(user_id)

    @property
    def web_session(self) -> bool:
        """The page showed the logged-in web app (a Direct link), not only the
        viewer cookie: Instagram serves such a session as the web client."""
        return bool(getattr(getattr(self.ctx, "tokens", None), "web_confirmed", False))

    def profile(self, username: str, *, user_id: str | None = None) -> dict[str, Any]:
        """Fetch (and cache) a raw profile record.

        A confirmed web session reads it from the profile page's own
        responses: Instagram does not serve it ``users/{id}/info`` or
        ``web_profile_info`` (429 without JSON on the first read, two
        accounts, 2026-09-29), which the web app never calls. Other cookie
        sessions with a known numeric id use ``users/{id}/info`` instead of
        ``web_profile_info`` (measured 2026-09-22: the latter answered 429,
        the id-keyed read 200); that record carries no inline timeline, so
        ``latestPosts`` stay empty on that path.
        """
        key = username.lower()
        self.remember_user_id(username, user_id)
        self._adopt_seed(key)
        known_id = self._user_ids.get(key)
        stale_visibility = (key in self._anonymous_profiles and
                            bool(self._profiles.get(key, {}).get("is_private")))
        if key not in self._profiles or (self.ctx.authenticated and stale_visibility):
            if self.web_session and self.ctx.authenticated:
                # the page is loaded by then: its og:description header
                # (rounded counts) is the last resort, not another request
                record = (page.read_app_profile(self.ctx, username)
                          or page.read_profile(self.ctx, username, navigate=False))
                if not record:
                    raise ExtractionError(
                        f"the profile page of @{username} did not receive its profile "
                        "(no PolarisProfilePageContentQuery response, no page header)")
                if key in self._profiles:
                    previous = self.viewer_independent_profile(dict(self._profiles[key]))
                    record = {**previous, **record}
            elif known_id and self.ctx.authenticated:
                record = self.api.user_by_id(known_id)
                if key in self._profiles:
                    # Preserve public timeline fields while replacing every
                    # viewer-dependent field with the authenticated response.
                    previous = dict(self._profiles[key])
                    previous = self.viewer_independent_profile(previous)
                    record = {**previous, **record}
            else:
                record = self.api.profile(username)
            self._profiles[key] = record
            self._user_ids[key] = str(record.get("id") or record.get("pk") or "")
            self._anonymous_profiles.discard(key)
        return self._profiles[key]

    def user_id(self, username: str, *, known_id: str | int | None = None) -> str:
        """Resolve a username to its numeric id.

        Use a verified ID from the target or session cache first. If absent,
        try the username profile endpoint, then the rendered profile page;
        the username endpoint can be throttled in authenticated contexts.
        """
        key = username.lower()
        self.remember_user_id(username, known_id)
        self._adopt_seed(key)
        if key in self._user_ids and self._user_ids[key]:
            return self._user_ids[key]

        lookup_error: InstagramError | None = None
        try:
            self.profile(username)
        except NotFoundError:
            raise
        except InstagramError as exc:
            lookup_error = exc
            log.info("profile lookup for @%s failed (%s); reading the id "
                     "off the page instead", username, type(exc).__name__)

        user_id = self._user_ids.get(key)
        if not user_id:
            user_id = page.read_user_id(self.ctx, username)
            if user_id:
                self._user_ids[key] = user_id

        if not user_id:
            if lookup_error:
                raise lookup_error
            raise NotFoundError(f"could not resolve a user id for @{username}")
        return user_id

    @property
    def authenticated(self) -> bool:
        return self.ctx.authenticated


class BaseScraper:
    """Common behaviour: date filtering, limits, uniform fallback logging."""

    #: label used in log lines and fallback stats
    name = "base"

    def __init__(self, scrape: ScrapeContext) -> None:
        self.scrape = scrape
        self.config = scrape.config
        self.api = scrape.api
        self.ai = scrape.ai
        self.ctx = scrape.ctx
        self.stats = scrape.stats

    # ------------------------------------------------------------------ API --

    def run(self, target: Target) -> Iterator[dict[str, Any]]:  # pragma: no cover
        raise NotImplementedError

    # ------------------------------------------------------------- filtering --

    def within_dates(self, record: dict[str, Any]) -> bool:
        """Apply `onlyPostsNewerThan` / `untilDate` to a mapped record."""
        return within_dates(self.config, record)

    def date_floor(self) -> "DateFloorDetector":
        """A stop-signal for paginating a feed under `onlyPostsNewerThan`."""
        return DateFloorDetector(self.config.only_posts_newer_than)

    def limited(self, records: Iterator[dict[str, Any]], limit: int) -> Iterator[dict[str, Any]]:
        """Yield at most `limit` records, counting them into the stats."""
        count = 0
        for record in records:
            yield record
            count += 1
            self.stats.items += 1
            if count >= limit:
                return

    # -------------------------------------------------------------- fallback --

    def note_fallback(self, reason: str) -> None:
        self.stats.fallbacks[f"{self.name}:{reason}"] += 1
        log.info("[%s] falling back to page scraping (%s)", self.name, reason)

    def should_fall_back(self, error: Exception) -> bool:
        """True when an error means "try the rendered page instead".

        `InstagramServerError` is included because the fault is Instagram's,
        not the target's: @natgeo's `web_profile_info` answers HTTP 400 with a
        deleted-schema message while its profile page renders perfectly, so
        giving up there would lose a scrapable account.
        """
        if isinstance(error, (LoginRequiredError, ChallengeRequiredError,
                              RateLimitedError, InstagramServerError)):
            return self.config.ai_fallback
        return False

    def fatal_for_target(self, error: Exception) -> bool:
        """Errors that mean this target can never produce results."""
        return isinstance(error, (NotFoundError, PrivateProfileError))


class DateFloorDetector:
    """Decides when a feed has genuinely paginated past `onlyPostsNewerThan`.

    A profile grid is *not* strictly newest-first: Instagram keeps up to three
    pinned posts at the top and they retain their original dates.  Stopping at
    the first out-of-range post therefore returns nothing at all whenever a
    pinned post predates the filter -- which is exactly what happened to
    @nasa, whose pinned post is weeks older than its newest ones.

    So pinned posts never trigger the stop, and an ordinary post only counts
    towards it while the run is unbroken: a single stale item in an otherwise
    fresh feed resets the count.
    """

    #: consecutive out-of-range posts required before we believe the feed has
    #: really passed the floor
    RUN_LENGTH = 5

    def __init__(self, floor: datetime | None, *, run_length: int | None = None) -> None:
        self.floor = floor
        self.run_length = run_length or self.RUN_LENGTH
        self.consecutive = 0

    def passed(self, record: dict[str, Any]) -> bool:
        """True once it is safe to stop paginating."""
        if not self.floor:
            return False
        if record.get("isPinned"):
            return False
        moment = _record_time(record)
        if moment is None or moment >= self.floor:
            self.consecutive = 0
            return False
        self.consecutive += 1
        return self.consecutive >= self.run_length


def within_dates(config: Any, record: dict[str, Any]) -> bool:
    """Apply `onlyPostsNewerThan` / `untilDate` to a mapped record (post or
    comment) by its own ``timestamp``; shared by the feed scrapers and the
    complete-mode collector."""
    newer, until = config.only_posts_newer_than, config.until_date
    if not newer and not until:
        return True
    moment = _record_time(record)
    if moment is None:
        # Undated records (AI fallback) are kept: dropping them would hide
        # data the user can still filter downstream.
        return True
    if newer and moment < newer:
        return False
    if until and moment >= until:
        return False
    return True


def _record_time(record: dict[str, Any]) -> datetime | None:
    value = record.get("timestamp")
    if not value:
        return None
    try:
        moment = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return moment if moment.tzinfo else moment.replace(tzinfo=timezone.utc)
