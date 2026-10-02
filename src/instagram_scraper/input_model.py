"""Actor input: parsing, validation and defaults.

The field names mirror `apify/instagram-scraper` so that an existing input JSON
can be dropped in unchanged.  Everything getbro-specific lives under the
`bro*` / `proxy*` keys and has sensible defaults.
"""

from __future__ import annotations

import json
import logging
import math
import os
import re
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from typing import Any
from urllib.parse import unquote

from .errors import InputError

log = logging.getLogger(__name__)

__all__ = [
    "ResultsType", "SearchType", "ProxyConfig", "BroConfig", "ScraperInput",
    "parse_date_filter", "cookie_viewer_id",
]


class ResultsType(str, Enum):
    POSTS = "posts"
    REELS = "reels"
    COMMENTS = "comments"
    MENTIONS = "mentions"
    DETAILS = "details"
    STORIES = "stories"


class SearchType(str, Enum):
    HASHTAG = "hashtag"
    USER = "user"
    PLACE = "place"


# `searchType: "profile"` is what the UI shows; the API historically used "user".
_SEARCH_ALIASES = {"profile": "user", "users": "user", "profiles": "user",
                   "hashtags": "hashtag", "tag": "hashtag", "tags": "hashtag",
                   "places": "place", "location": "place", "locations": "place"}

_REL_DATE_RE = re.compile(
    r"^\s*(\d+)\s*(second|minute|hour|day|week|month|year)s?\s*(?:ago)?\s*$", re.I
)
_UNIT_DAYS = {"second": 1 / 86400, "minute": 1 / 1440, "hour": 1 / 24,
              "day": 1, "week": 7, "month": 30.436875, "year": 365.25}


def parse_date_filter(value: str | int | float | datetime | None,
                      *, now: datetime | None = None) -> datetime | None:
    """Parse `onlyPostsNewerThan` / `untilDate` into an aware UTC datetime.

    Accepts relative spans (``"3 days"``, ``"2 months ago"``), ISO dates
    (``"2025-01-31"``), full ISO timestamps and unix epochs.
    """
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return datetime.fromtimestamp(float(value), tz=timezone.utc)

    text = str(value).strip()
    reference = now or datetime.now(timezone.utc)

    match = _REL_DATE_RE.match(text)
    if match:
        amount, unit = int(match.group(1)), match.group(2).lower()
        return reference - timedelta(days=amount * _UNIT_DAYS[unit])

    if text.isdigit() and len(text) >= 9:  # unix epoch as a string
        return datetime.fromtimestamp(int(text), tz=timezone.utc)

    iso = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(iso)
    except ValueError:
        raise InputError(
            f"cannot parse date {value!r}; use '3 days', 'YYYY-MM-DD' "
            "or 'YYYY-MM-DDTHH:mm:ssZ'"
        ) from None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


@dataclass(slots=True)
class ProxyConfig:
    """getbro residential-proxy settings for the browser session.

    Every browser goes through the residential proxy. getbro's own provider
    rotation is never requested: a browser keeps its exit IP, and the runner
    pins a run with cookies to one DE exit (see ``ScraperRun.pin_route``).
    """

    tier: str = "basic"           # lite | basic | premium
    policy: str = "extended"      # html_only | basic | extended | full
    #: ISO code; ``None`` leaves an anonymous browser on getbro's default exit (UK)
    country: str | None = None
    city: str | None = None
    block_unproxied: bool = False

    _TIERS = ("lite", "basic", "premium")
    _POLICIES = ("html_only", "basic", "extended", "full")

    def validate(self) -> None:
        if self.tier not in self._TIERS:
            raise InputError(f"proxyTier must be one of {self._TIERS}, got {self.tier!r}")
        if self.policy not in self._POLICIES:
            raise InputError(f"proxyPolicy must be one of {self._POLICIES}, got {self.policy!r}")
        if self.country and not re.fullmatch(r"[A-Za-z]{2}", self.country):
            raise InputError(f"proxyCountry must be a 2-letter ISO code, got {self.country!r}")

    def to_session_kwargs(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "enable_proxy": True,
            "proxy_tier": self.tier,
            "proxy_policy": self.policy,
            "block_unproxied": self.block_unproxied,
        }
        if self.country:
            payload["country"] = self.country.upper()
        if self.city:
            payload["city"] = self.city
        return payload


#: getbro's ceiling for one browser's lifetime (6 hours); the default stays
#: at one hour. A session also ends after 300 s with no command queued or
#: running -- that idle limit is separate from the lifetime and is not
#: extended by anything here.
MAX_SESSION_TIMEOUT = 21600.0
DEFAULT_SESSION_TIMEOUT = 3600.0

#: the ``har_filtered`` response source exports only these resource types.
#: The plan started from ``document``/``xhr``/``fetch``; the controlled probe
#: of 15 September (verification/archive-2026-09-30/tools/har_since_probe.py)
#: showed getbro's router filing
#: a page ``fetch()``, an ``XMLHttpRequest`` and its own ``fetch_json`` under
#: ``other`` -- only a navigation is ``document`` -- so ``other`` is part of
#: the filter, or no comment page would be exported. What stays out is the
#: static weight: scripts, stylesheets, images, media, fonts.
HAR_FILTER_RESOURCE_TYPES = ("document", "xhr", "fetch", "other")


#: getbro read timeout, seconds (see ``BroConfig.read_timeout``)
DEFAULT_READ_TIMEOUT = 90.0

#: VM swaps an anonymous run may make behind a login wall (``maxIpRotations``).
#: Measured 2026-09-30, one anonymous browser over 40 profiles: 8 swaps
#: (no country pinned: getbro's default UK exits) reached every profile
#: Instagram serves logged out.
DEFAULT_IP_ROTATIONS = 10


@dataclass(slots=True)
class BroConfig:
    """How to talk to getbro."""

    api_key: str = ""
    base_url: str = "https://api.getbro.ws"
    session_timeout: float = DEFAULT_SESSION_TIMEOUT
    #: how long to wait for one command batch before giving up, seconds
    command_timeout: float = 300.0
    recovery_timeout: float = 120.0
    #: parallel browser sessions; each is a separate VM and is billed separately
    concurrency: int = 1
    skip_balance_check: bool = False
    #: seconds a command may stay queued before that is logged as a slow
    #: dispatch; a diagnostic threshold, not a verdict -- the command keeps
    #: being observed under its own id
    dispatch_timeout: float = 45.0
    #: seconds getbro lets one Instagram read (``fetch_json``) run before
    #: aborting it (getbro's own default is 30 s). Lite exits answered first
    #: comment pages past 30 s; with 90 s none timed out (2026-09-25).
    #: Range 0.1-180 (SDK 0.1.8).
    read_timeout: float | None = DEFAULT_READ_TIMEOUT

    def validate(self) -> None:
        if not self.api_key:
            raise InputError(
                "getbro API key missing: set BRO_API_KEY in the environment "
                "or pass `broApiKey` in the input"
            )
        if self.concurrency < 1:
            raise InputError("broConcurrency must be >= 1")
        if not math.isfinite(self.session_timeout) or self.session_timeout <= 0:
            raise InputError("broSessionTimeout must be finite and > 0")
        if self.session_timeout > MAX_SESSION_TIMEOUT:
            raise InputError(f"broSessionTimeout cannot exceed {MAX_SESSION_TIMEOUT:.0f} seconds")
        if self.read_timeout is not None and not (
                math.isfinite(self.read_timeout) and 0.1 <= self.read_timeout <= 180):
            raise InputError(f"broReadTimeout must be between 0.1 and 180 seconds, got {self.read_timeout!r}")
        if not math.isfinite(self.dispatch_timeout) or self.dispatch_timeout <= 0:
            raise InputError("broDispatchTimeout must be finite and > 0")


@dataclass(slots=True)
class ScraperInput:
    """Fully validated actor input."""

    # --- Apify-compatible fields -------------------------------------------
    direct_urls: list[str] = field(default_factory=list)
    results_type: ResultsType = ResultsType.POSTS
    results_limit: int = 200
    search: str = ""
    search_type: SearchType = SearchType.HASHTAG
    search_limit: int = 10
    only_posts_newer_than: datetime | None = None
    until_date: datetime | None = None
    add_parent_data: bool = False
    skip_pinned_posts: bool = False
    is_newest_comments: bool = False
    #: Complete chronological comments: read every post's comments from the
    #: first opened post document (page-context reads by media id) instead of
    #: opening each post's page. Measured 15,840 records from 12 posts in one
    #: VM (2026-09-29). ``False`` restores one navigation per post. No effect
    #: on other modes.
    reuse_post_document: bool = True
    include_nested_comments: bool = False
    add_profile_statistics: bool = False
    enhance_user_search_with_facebook_page: bool = False
    expand_owners: bool = False

    # --- engine fields ------------------------------------------------------
    #: the run's first (or only) Instagram account
    session_cookies: list[dict[str, Any]] = field(default_factory=list)
    #: further accounts (``sessionCookiesList``), one cookie jar each. Every
    #: account gets its own browser and exit IP; its cookies go into that
    #: browser once and never into another one.
    extra_accounts: list[list[dict[str, Any]]] = field(default_factory=list)
    ai_fallback: bool = True
    ai_model_size: str = "small"
    #: Re-read AI-discovered posts through Instagram's embed renderer to fill
    #: in caption, owner and media URL.  One extra page load per post.
    enrich_from_embed: bool = False
    #: Stop the run when `sessionCookies` were supplied but Instagram serves
    #: the logged-out page.  Degrading silently is worse than failing: one
    #: measured run spent 724s and $0.26 producing 33 caption-less, like-less
    #: records through the vision fallback, when the real answer was "your
    #: sessionid is dead".  Set False to accept the degraded result instead.
    require_valid_session: bool = True
    #: Read profile ids anonymously before the cookies go in (the pre-web-
    #: session workaround for a 429 on the authenticated username lookup).
    #: Off by default: a logged-in web session reads profiles from their page,
    #: and one anonymous exit IP hits the login wall after ~14 profiles, after
    #: which each id costs a page load -- 26 minutes for 37 profiles, 29.09.
    anonymous_profile_ids: bool = False
    #: Press "Continue" once when the cookies land on Instagram's saved-account
    #: screen (the account is recognised, the web session waits for the tap).
    #: Nothing is ever typed; a password prompt still stops the run.
    resume_saved_login: bool = True
    #: VM swaps an anonymous run may make, all its browsers together, when
    #: Instagram puts an exit IP behind its login wall (or throttles it).
    #: Each swap stops the browser's VM and boots a fresh one with a new exit
    #: IP; the blocked read is repeated there. A run with cookies never swaps.
    max_ip_rotations: int = DEFAULT_IP_ROTATIONS
    #: Seconds to wait after each scroll step while harvesting a profile grid.
    grid_scroll_pause: float = 2.0
    #: Hard ceiling on scroll steps per grid harvest.
    grid_max_scrolls: int = 60
    #: Seconds between per-post `media/info` lookups, jittered.  Rebuilding a
    #: grid post-by-post is far more request-heavy than the browsing it
    #: imitates -- a real client fetches ~1 request per 12 tiles -- and running
    #: it flat out is what gets an account flagged, so pace it deliberately.
    media_lookup_pause: float = 1.2
    authenticated_request_pause: float = 3.0
    max_comment_pages: int = 100
    capture_network_responses: bool = True
    #: Comment preloading adds one post navigation; opt in until live verified.
    capture_network_comments: bool = False
    max_replies_per_comment: int | None = None
    comment_post_limit: int | None = None
    collection_mode: str = "standard"
    max_api_requests: int | None = None
    max_enrichment_requests: int | None = None
    collection_phase: str = "all"
    #: Start another explicit popular-parent pass in a new resumed invocation.
    #: Instagram's ranked sample can differ between browser sessions.
    revisit_ranked_parents_on_resume: bool = False
    #: Where complete-mode comment pages come from in the browser: the
    #: in-page fetch/XHR observer (``observer``), cumulative HAR dumps
    #: (``har``) or cumulative HAR dumps exported with a resource-type filter
    #: (``har_filtered``, see :data:`HAR_FILTER_RESOURCE_TYPES`). The observer
    #: is the default; ``har`` keeps the old path, ``har_filtered`` is the
    #: lighter export under evaluation.
    response_source: str = "observer"
    max_run_seconds: float | None = None
    resume: Path | None = None
    materialize_items: bool = True
    proxy: ProxyConfig = field(default_factory=ProxyConfig)
    bro: BroConfig = field(default_factory=BroConfig)
    dataset_name: str | None = None
    output_dir: Path = Path("storage")
    output_format: str = "json"   # json | jsonl | csv
    max_request_retries: int = 1
    debug: bool = False

    # ------------------------------------------------------------------ #

    @property
    def targets_requested(self) -> bool:
        return bool(self.direct_urls) or bool(self.search.strip())

    def account_jars(self) -> list[list[dict[str, Any]]]:
        """Every account of the run, first one first; empty when anonymous."""
        return ([self.session_cookies] if self.session_cookies else []) + list(self.extra_accounts)

    def pin_session_to_one_ip(self) -> list[str]:
        """Keep every account on one browser and one exit IP.

        A `sessionid` replayed from many residential IPs in quick succession is
        exactly what account-takeover detection looks for. Doing it got a real
        test account locked out with a "suspicious login" notice and a forced
        password reset. So a run with cookies never swaps VMs
        (``maxIpRotations`` becomes 0) and runs one browser per account; the
        runner pins the route to one DE exit.

        Returns a list of human-readable notes about what was changed.
        """
        notes: list[str] = []
        accounts = len(self.account_jars())
        if not accounts:
            return notes
        # Not a note: VM swaps exist for anonymous runs only.
        self.max_ip_rotations = 0
        if self.bro.concurrency != accounts:
            self.bro.concurrency = accounts
            notes.append("broConcurrency set to 1 to reuse one authenticated browser"
                         if accounts == 1 else
                         f"broConcurrency set to {accounts}: one browser per account, "
                         "each account's cookies in its own browser only")
        return notes

    def validate(self) -> None:
        if self.collection_phase not in ("all", "collect", "enrich"):
            raise InputError("collectionPhase must be all, collect or enrich")
        if self.collection_phase != "all" and self.collection_mode != "complete":
            raise InputError("collectionPhase requires collectionMode complete")
        if self.collection_phase == "enrich" and not self.resume:
            raise InputError("collectionPhase enrich requires resume")
        if self.max_enrichment_requests is not None and self.max_enrichment_requests < 0:
            raise InputError("maxEnrichmentRequests must be >= 0")
        if self.collection_mode not in ("standard", "complete"):
            raise InputError("collectionMode must be standard or complete")
        if self.resume and self.collection_mode != "complete":
            raise InputError("resume requires collectionMode complete")
        if self.max_api_requests is not None and self.max_api_requests < 1:
            raise InputError("maxApiRequests must be >= 1")
        if self.max_request_retries < 0:
            raise InputError("maxRequestRetries must be >= 0")
        if not math.isfinite(self.bro.recovery_timeout) or self.bro.recovery_timeout < 0:
            raise InputError("broRecoveryTimeout must be a finite number >= 0")
        if self.max_run_seconds is not None and (not math.isfinite(self.max_run_seconds) or self.max_run_seconds <= 0):
            raise InputError("maxRunSeconds must be finite and > 0")
        if not self.targets_requested:
            raise InputError("provide `directUrls` and/or a `search` query")
        if self.extra_accounts and not self.session_cookies:
            raise InputError("further accounts need a first account in sessionCookies")
        jars = self.account_jars()
        _check_distinct_accounts(jars)
        if len(jars) > 1 and self.collection_mode == "complete":
            if not self.direct_urls:
                raise InputError("several accounts in complete mode need directUrls: each "
                                 "account collects its own share of them, and a search is not split")
            unknown = [str(index) for index, jar in enumerate(jars, 1) if not cookie_viewer_id(jar)]
            if unknown:
                raise InputError(
                    f"account {', '.join(unknown)} of sessionCookiesList: neither a ds_user_id "
                    "cookie nor an account id in the sessionid; complete mode with several "
                    "accounts needs it to hand each saved share back to its own account")
        if self.results_limit < 1:
            raise InputError("resultsLimit must be >= 1")
        if self.search_limit < 1:
            raise InputError("searchLimit must be >= 1")
        if self.output_format not in ("json", "jsonl", "csv"):
            raise InputError("outputFormat must be json, jsonl or csv")
        if self.ai_model_size not in ("small", "medium", "large"):
            raise InputError("aiModelSize must be small, medium or large")
        if self.max_ip_rotations < 0:
            raise InputError("maxIpRotations cannot be negative")
        if self.max_comment_pages < 1:
            raise InputError("maxCommentPages must be >= 1")
        if self.response_source not in ("observer", "har", "har_filtered"):
            raise InputError("responseSource must be observer, har or har_filtered")
        if self.grid_max_scrolls < 0:
            raise InputError("gridMaxScrolls must be >= 0")
        if self.max_replies_per_comment is not None and self.max_replies_per_comment < 0:
            raise InputError("maxRepliesPerComment must be >= 0")
        if self.comment_post_limit is not None and self.comment_post_limit < 1:
            raise InputError("commentPostLimit must be >= 1")
        for name in ("authenticated_request_pause", "media_lookup_pause", "grid_scroll_pause"):
            value = getattr(self, name)
            if not math.isfinite(value) or value < 0:
                raise InputError(f"{name} must be a finite nonnegative number")
        if (self.only_posts_newer_than and self.until_date
                and self.only_posts_newer_than >= self.until_date):
            raise InputError("onlyPostsNewerThan must be earlier than untilDate")
        self.proxy.validate()
        self.bro.validate()
        if self.dataset_name is not None:
            if not self.dataset_name or any(c in self.dataset_name for c in r"\/:*?\"<>|"):
                raise InputError(f"dataset name contains invalid characters: {self.dataset_name}")
            if ".." in self.dataset_name:
                raise InputError(f"dataset name contains invalid characters: {self.dataset_name}")

    def to_dict(self) -> dict[str, Any]:
        """JSON-serialisable snapshot, with the API key redacted."""
        data = asdict(self)
        data["results_type"] = self.results_type.value
        data["search_type"] = self.search_type.value
        data["output_dir"] = str(self.output_dir)
        data["dataset_name"] = self.dataset_name
        data["resume"] = str(self.resume) if self.resume else None
        for key in ("only_posts_newer_than", "until_date"):
            value = getattr(self, key)
            data[key] = value.isoformat() if value else None
        data["bro"]["api_key"] = "***" if self.bro.api_key else ""
        data["session_cookies"] = f"<{len(self.session_cookies)} cookies>"
        data["extra_accounts"] = f"<{len(self.extra_accounts)} accounts>"
        return data


# --------------------------------------------------------------------------- #
# Construction from raw JSON
# --------------------------------------------------------------------------- #

def _get(raw: dict[str, Any], *names: str, default: Any = None) -> Any:
    """First present key out of `names` (supports camelCase and snake_case)."""
    for name in names:
        if name in raw and raw[name] is not None:
            return raw[name]
        snake = re.sub(r"(?<!^)(?=[A-Z])", "_", name).lower()
        if snake in raw and raw[snake] is not None:
            return raw[snake]
    return default


def _as_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in ("1", "true", "yes", "on")


def _as_int(value: Any, default: int) -> int:
    if value is None or value == "":
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        raise InputError(f"expected an integer, got {value!r}") from None


def _as_url_list(value: Any) -> list[str]:
    """Accept a list of strings, a list of `{url: ...}` objects, or one string."""
    if value is None:
        return []
    if isinstance(value, str):
        return [part.strip() for part in value.replace("\n", ",").split(",") if part.strip()]
    if isinstance(value, dict):
        value = [value]
    out: list[str] = []
    for item in value:
        if isinstance(item, str) and item.strip():
            out.append(item.strip())
        elif isinstance(item, dict):
            url = item.get("url") or item.get("Url") or item.get("URL")
            if url:
                out.append(str(url).strip())
    return out


#: what ``ScraperInput.to_dict`` writes instead of cookies (saved INPUT.json)
_REDACTED = re.compile(r"<\d+ (?:cookies|accounts)>")


def _redacted(value: Any) -> bool:
    if isinstance(value, str) and _REDACTED.fullmatch(value.strip()):
        log.warning("ignoring %s: a saved input keeps no cookies; supply them again", value.strip())
        return True
    return False


def _normalise_cookies(value: Any) -> list[dict[str, Any]]:
    """Accept an array of cookie objects, a cookie header string, or a sessionid."""
    if not value or _redacted(value):
        return []
    if isinstance(value, dict):
        value = [value]
    if isinstance(value, str):
        text = value.strip()
        if text.startswith("["):
            try:
                value = json.loads(text)
            except json.JSONDecodeError:
                raise InputError("sessionCookies is not valid JSON") from None
        else:
            # "sessionid=abc; csrftoken=def"  or just the raw sessionid
            cookies: list[dict[str, Any]] = []
            if "=" not in text:
                cookies.append({"name": "sessionid", "value": text})
            else:
                for part in text.split(";"):
                    if "=" not in part:
                        continue
                    name, _, val = part.partition("=")
                    cookies.append({"name": name.strip(), "value": val.strip()})
            value = cookies

    out: list[dict[str, Any]] = []
    for cookie in value:
        if not isinstance(cookie, dict) or not cookie.get("name"):
            continue
        out.append({
            "name": str(cookie["name"]),
            "value": str(cookie.get("value", "")),
            "domain": str(cookie.get("domain") or ".instagram.com"),
            "path": str(cookie.get("path") or "/"),
        })
    return out


def _normalise_accounts(value: Any) -> list[list[dict[str, Any]]]:
    """``sessionCookiesList``: one entry per Instagram account.

    A list whose entries are anything ``sessionCookies`` accepts (a
    ``sessionid``, a ``"a=b; c=d"`` header, an array of cookie objects or its
    JSON text, or ``{"cookies": ...}``), or one string: a JSON array of such
    entries, or one account per line.
    """
    if value is None or value == "" or value == [] or _redacted(value):
        return []
    if isinstance(value, str):
        text = value.strip()
        if text.startswith("["):
            try:
                value = json.loads(text)
            except json.JSONDecodeError:
                raise InputError("sessionCookiesList is not valid JSON") from None
        else:
            value = [line.strip() for line in text.splitlines() if line.strip()]
    if isinstance(value, dict):
        value = [value]
    if not isinstance(value, list):
        raise InputError("sessionCookiesList must list one entry per account")
    if all(isinstance(entry, dict) and "name" in entry for entry in value):
        # One account's cookie export pasted where a list of accounts belongs.
        value = [value]
    accounts: list[list[dict[str, Any]]] = []
    for number, entry in enumerate(value, 1):
        if isinstance(entry, dict) and "cookies" in entry:
            entry = entry["cookies"]
        jar = _normalise_cookies(entry)
        if not jar:
            raise InputError(f"sessionCookiesList entry {number} holds no cookies")
        accounts.append(jar)
    return accounts


def cookie_viewer_id(cookies: list[dict[str, Any]]) -> str | None:
    """The Instagram account id a cookie jar belongs to, when it says so.

    ``ds_user_id`` names it; a ``sessionid`` also starts with it
    (``<id>%3A<token>...``). Nothing is read from the network.
    """
    values = {str(cookie.get("name")): str(cookie.get("value") or "") for cookie in cookies or []}
    viewer = values.get("ds_user_id", "").strip()
    if viewer.isdigit():
        return viewer
    session = unquote(values.get("sessionid", "")).strip()
    head, separator, _ = session.partition(":")
    return head if separator and head.isdigit() else None


def _check_distinct_accounts(jars: list[list[dict[str, Any]]]) -> None:
    """One Instagram account must never run in two browsers at once."""
    seen: dict[str, int] = {}
    for number, jar in enumerate(jars, 1):
        viewer = cookie_viewer_id(jar)
        session = next((str(c.get("value")) for c in jar if c.get("name") == "sessionid"), "")
        for key in ((f"viewer:{viewer}" if viewer else ""), (f"session:{session}" if session else "")):
            if not key:
                continue
            if key in seen:
                raise InputError(
                    f"account {number} of the run is the same Instagram account as account "
                    f"{seen[key]}: one account runs in one browser only")
            seen[key] = number


def build_input(raw: dict[str, Any] | None = None, *, env: dict[str, str] | None = None) -> ScraperInput:
    """Build a validated :class:`ScraperInput` from raw JSON plus environment."""
    raw = dict(raw or {})
    env = env if env is not None else dict(os.environ)

    results_type = str(_get(raw, "resultsType", default="posts")).strip().lower()
    if results_type in ("post", "reel"):
        results_type += "s"
    if results_type not in {t.value for t in ResultsType}:
        raise InputError(
            f"resultsType must be one of {sorted(t.value for t in ResultsType)}, "
            f"got {results_type!r}"
        )

    search_type = str(_get(raw, "searchType", default="hashtag")).strip().lower()
    search_type = _SEARCH_ALIASES.get(search_type, search_type)
    if search_type not in {t.value for t in SearchType}:
        raise InputError(
            f"searchType must be one of {sorted(t.value for t in SearchType)} "
            f"(or 'profile'), got {search_type!r}"
        )

    proxy_raw = _get(raw, "proxyConfiguration", default={}) or {}
    proxy = ProxyConfig(
        tier=str(_get(raw, "proxyTier", default=proxy_raw.get("tier") or "basic")).lower(),
        policy=str(_get(raw, "proxyPolicy", default=proxy_raw.get("policy") or "extended")).lower(),
        # No country chosen: an anonymous run takes getbro's default exit (UK); a run with
        # cookies is pinned to DE by the runner.
        country=(_get(raw, "proxyCountry", default=proxy_raw.get("countryCode")) or None),
        city=_get(raw, "proxyCity", default=proxy_raw.get("city")),
        block_unproxied=_as_bool(_get(raw, "blockUnproxied"), False),
    )

    bro = BroConfig(
        api_key=str(_get(raw, "broApiKey", default="") or env.get("BRO_API_KEY", "")).strip(),
        base_url=str(_get(raw, "broBaseUrl",
                          default=env.get("BRO_BASE_URL", "https://api.getbro.ws"))).rstrip("/"),
        session_timeout=float(_get(raw, "broSessionTimeout", default=DEFAULT_SESSION_TIMEOUT)),
        command_timeout=float(_get(raw, "broCommandTimeout", default=300.0)),
        recovery_timeout=float(_get(raw, "broRecoveryTimeout", default=120.0)),
        concurrency=_as_int(_get(raw, "broConcurrency", "concurrency"), 1),
        skip_balance_check=_as_bool(_get(raw, "broSkipBalanceCheck")),
        dispatch_timeout=float(_get(raw, "broDispatchTimeout", default=45.0)),
        read_timeout=float(_get(raw, "broReadTimeout", default=DEFAULT_READ_TIMEOUT)),
    )

    cookies = _normalise_cookies(_get(raw, "sessionCookies", "cookies"))
    more_accounts = _normalise_accounts(_get(raw, "sessionCookiesList"))
    if not cookies and not more_accounts and env.get("IG_SESSIONID"):
        cookies = _normalise_cookies(env["IG_SESSIONID"])
    if not cookies and more_accounts:
        cookies = more_accounts.pop(0)

    dataset_name = _get(raw, "datasetName", "dataset_name", "datasetId", "dataset_id", "runId", "run_id")
    if not dataset_name:
        dataset_name = env.get("DATASET_NAME") or env.get("DATASET_ID") or env.get("RUN_ID") or env.get("ACTOR_DEFAULT_DATASET_ID") or env.get("APIFY_DEFAULT_DATASET_ID")

    parsed = ScraperInput(
        direct_urls=_as_url_list(_get(raw, "directUrls", "startUrls", "urls")),
        results_type=ResultsType(results_type),
        results_limit=_as_int(_get(raw, "resultsLimit"), 200),
        search=str(_get(raw, "search", "searchQuery", default="") or "").strip(),
        search_type=SearchType(search_type),
        search_limit=_as_int(_get(raw, "searchLimit"), 10),
        only_posts_newer_than=parse_date_filter(_get(raw, "onlyPostsNewerThan", "fromDate")),
        until_date=parse_date_filter(_get(raw, "untilDate", "toDate")),
        add_parent_data=_as_bool(_get(raw, "addParentData")),
        skip_pinned_posts=_as_bool(_get(raw, "skipPinnedPosts")),
        is_newest_comments=_as_bool(_get(raw, "isNewestComments")),
        reuse_post_document=_as_bool(_get(raw, "reusePostDocument"), True),
        include_nested_comments=_as_bool(_get(raw, "includeNestedComments")),
        collection_mode=str(_get(raw, "collectionMode", default="standard")),
        collection_phase=str(_get(raw, "collectionPhase", default="all")),
        revisit_ranked_parents_on_resume=_as_bool(
            _get(raw, "revisitRankedParentsOnResume")),
        response_source=str(_get(raw, "responseSource", default="observer")).lower().strip(),
        max_enrichment_requests=(_as_int(_get(raw, "maxEnrichmentRequests"), 0)
                                 if _get(raw, "maxEnrichmentRequests") is not None else None),
        max_api_requests=(_as_int(_get(raw, "maxApiRequests"), 200)
                          if _get(raw, "maxApiRequests") is not None else None),
        max_run_seconds=(float(_get(raw, "maxRunSeconds"))
                         if _get(raw, "maxRunSeconds") is not None else None),
        resume=Path(_get(raw, "resume")) if _get(raw, "resume") else None,
        materialize_items=_as_bool(_get(raw, "materializeItems", default=True)),
        capture_network_responses=_as_bool(_get(raw, "captureNetworkResponses", default=True)),
        capture_network_comments=_as_bool(_get(raw, "captureNetworkComments")),
        max_replies_per_comment=(_as_int(_get(raw, "maxRepliesPerComment"), 0)
                                 if _get(raw, "maxRepliesPerComment") is not None else None),
        comment_post_limit=(_as_int(_get(raw, "commentPostLimit"), 1)
                            if _get(raw, "commentPostLimit") is not None else None),
        add_profile_statistics=_as_bool(_get(raw, "addProfileStatistics")),
        enhance_user_search_with_facebook_page=_as_bool(
            _get(raw, "enhanceUserSearchWithFacebookPage")),
        expand_owners=_as_bool(_get(raw, "expandOwners")),
        session_cookies=cookies,
        extra_accounts=more_accounts,
        ai_fallback=_as_bool(_get(raw, "aiFallback"), True),
        ai_model_size=str(_get(raw, "aiModelSize", default="small")).lower(),
        enrich_from_embed=_as_bool(_get(raw, "enrichFromEmbed")),
        require_valid_session=_as_bool(_get(raw, "requireValidSession"), True),
        resume_saved_login=_as_bool(_get(raw, "resumeSavedLogin"), True),
        anonymous_profile_ids=_as_bool(_get(raw, "anonymousProfileIds"), False),
        max_ip_rotations=_as_int(_get(raw, "maxIpRotations"), DEFAULT_IP_ROTATIONS),
        grid_scroll_pause=float(_get(raw, "gridScrollPause", default=2.0)),
        grid_max_scrolls=_as_int(_get(raw, "gridMaxScrolls"), 60),
        media_lookup_pause=float(_get(raw, "mediaLookupPause", default=1.2)),
        authenticated_request_pause=float(_get(raw, "authenticatedRequestPause", default=3.0)),
        max_comment_pages=_as_int(_get(raw, "maxCommentPages"), 100),
        proxy=proxy,
        bro=bro,
        dataset_name=str(dataset_name) if dataset_name else None,
        output_dir=Path(str(_get(raw, "outputDir", default=env.get("OUTPUT_DIR", "storage")))),
        output_format=str(_get(raw, "outputFormat", default="json")).lower(),
        max_request_retries=_as_int(_get(raw, "maxRequestRetries"), 1),
        debug=_as_bool(_get(raw, "debug"), False),
    )
    parsed.validate()
    return parsed


def load_input_file(path: str | Path) -> dict[str, Any]:
    """Read an actor `INPUT.json`."""
    file = Path(path)
    if not file.exists():
        raise InputError(f"input file not found: {file}")
    try:
        return json.loads(file.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise InputError(f"{file} is not valid JSON: {exc}") from None
