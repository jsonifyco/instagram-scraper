"""Classification and normalisation of Instagram URLs.

`directUrls` accepts anything a user can copy out of the address bar.  This
module turns each entry into a :class:`Target` describing *what* it points at,
so the scrapers never have to re-parse strings.
"""

from __future__ import annotations

import re
from typing import Any
from dataclasses import dataclass, field
from enum import Enum
from urllib.parse import unquote, urlparse

from .errors import UnsupportedUrlError

__all__ = ["TargetType", "Target", "parse_target", "parse_targets", "post_url", "profile_url"]

_HOSTS = {
    "instagram.com",
    "www.instagram.com",
    "m.instagram.com",
    "l.instagram.com",
    "instagr.am",
    "www.instagr.am",
    "ig.me",
}

# Path prefixes that are Instagram product pages, never usernames.
_RESERVED = {
    "p", "reel", "reels", "tv", "explore", "stories", "s", "accounts", "direct",
    "about", "developer", "legal", "privacy", "terms", "api", "graphql",
    "challenge", "emails", "session", "web", "ajax", "static", "your_activity",
    "download", "lite", "igtv", "topics", "locations", "tags", "creators",
}

_USERNAME_RE = re.compile(r"^[A-Za-z0-9._]{1,30}$")
_NUMERIC_RE = re.compile(r"^\d+$")


class TargetType(str, Enum):
    """What a `directUrls` entry points at."""

    PROFILE = "profile"
    POST = "post"
    REEL = "reel"
    IGTV = "igtv"
    HASHTAG = "hashtag"
    PLACE = "place"
    STORY = "story"
    TAGGED_FEED = "tagged"
    REELS_FEED = "reels_feed"
    SEARCH = "search"

    @property
    def is_media(self) -> bool:
        return self in (TargetType.POST, TargetType.REEL, TargetType.IGTV)

    @property
    def is_feed(self) -> bool:
        """True when the target yields a list of posts rather than one item."""
        return self in (
            TargetType.PROFILE,
            TargetType.HASHTAG,
            TargetType.PLACE,
            TargetType.TAGGED_FEED,
            TargetType.REELS_FEED,
        )


@dataclass(slots=True)
class Target:
    """A normalised scrape target."""

    type: TargetType
    #: username / shortcode / hashtag name / location id / search term
    key: str
    #: the URL exactly as the user supplied it (echoed back as `inputUrl`)
    input_url: str
    #: canonical instagram.com URL for this target
    url: str
    #: extra bits parsed out of the URL (e.g. location slug, story id)
    extra: dict[str, str] = field(default_factory=dict)

    def __str__(self) -> str:  # pragma: no cover - debugging aid
        return f"{self.type.value}:{self.key}"


def _segments(raw: str) -> tuple[list[str], str]:
    """Split a user-supplied string into path segments plus its query."""
    text = raw.strip()
    if not text:
        raise UnsupportedUrlError("empty URL")

    # Bare handles: "@nasa" or "nasa"
    if text.startswith("@"):
        text = f"https://www.instagram.com/{text[1:]}"
    elif "://" not in text and "." not in text.split("/")[0]:
        text = f"https://www.instagram.com/{text}"
    elif "://" not in text:
        text = f"https://{text}"

    parsed = urlparse(text)
    host = (parsed.netloc or "").lower().split("@")[-1].split(":")[0]
    if host and host not in _HOSTS:
        raise UnsupportedUrlError(f"not an Instagram URL: {raw!r}")

    segs = [unquote(s) for s in parsed.path.split("/") if s]
    return segs, parsed.query


def parse_target(raw: str) -> Target:
    """Classify one URL (or bare username / hashtag).

    Raises:
        UnsupportedUrlError: when the URL is not an Instagram URL we handle.
    """
    original = raw.strip()
    segs, query = _segments(original)
    if not segs:
        raise UnsupportedUrlError(f"no path in URL: {raw!r}")

    head = segs[0].lower()

    # /p/<code>/  /reel/<code>/  /reels/<code>/  /tv/<code>/
    if head in ("p", "reel", "reels", "tv") and len(segs) >= 2:
        code = segs[1]
        if code.isdigit() and len(code) >= 15:
            from .shortcode import media_id_to_shortcode
            code = media_id_to_shortcode(code)
        kind = {
            "p": TargetType.POST,
            "reel": TargetType.REEL,
            "reels": TargetType.REEL,
            "tv": TargetType.IGTV,
        }[head]
        return Target(kind, code, original, post_url(code, head))

    if head == "explore" and len(segs) >= 2:
        second = segs[1].lower()
        # /explore/tags/<name>/
        if second in ("tags", "tag") and len(segs) >= 3:
            tag = segs[2].lstrip("#").lower()
            return Target(
                TargetType.HASHTAG, tag, original,
                f"https://www.instagram.com/explore/tags/{tag}/",
            )
        # /explore/locations/<id>/<slug>/
        if second in ("locations", "location") and len(segs) >= 3:
            loc_id = segs[2]
            extra = {"slug": segs[3]} if len(segs) >= 4 else {}
            slug = f"{segs[3]}/" if len(segs) >= 4 else ""
            return Target(
                TargetType.PLACE, loc_id, original,
                f"https://www.instagram.com/explore/locations/{loc_id}/{slug}",
                extra,
            )
        # /explore/search/keyword/?q=...
        if second == "search":
            term = ""
            for part in query.split("&"):
                if part.startswith("q="):
                    term = unquote(part[2:]).replace("+", " ")
            if term:
                return Target(
                    TargetType.SEARCH, term, original,
                    f"https://www.instagram.com/explore/search/keyword/?q={term}",
                )
        raise UnsupportedUrlError(f"unsupported explore URL: {raw!r}")

    # /stories/<username>/ ; a highlight id is not a username.
    if head == "stories" and len(segs) >= 2:
        if segs[1].lower() == "highlights" and len(segs) >= 3:
            raise UnsupportedUrlError(
                f"individual story highlights are not implemented: {raw!r}")
        extra = {"kind": "user"}
        if len(segs) >= 3 and _NUMERIC_RE.match(segs[2]):
            extra["story_id"] = segs[2]
        return Target(
            TargetType.STORY, segs[1], original,
            f"https://www.instagram.com/stories/{segs[1]}/", extra,
        )

    if head in _RESERVED:
        raise UnsupportedUrlError(f"unsupported Instagram URL: {raw!r}")

    username = segs[0]
    if _NUMERIC_RE.match(username) and len(segs) == 1:
        # A bare numeric id names a profile by user id (as in Apify's
        # `directUrls`); the username is resolved when the target is scraped.
        return Target(TargetType.PROFILE, username, original, profile_url(username),
                      {"user_id": username})
    if not _USERNAME_RE.match(username):
        raise UnsupportedUrlError(f"not a valid username in {raw!r}")

    # /<username>/tagged/  and  /<username>/reels/
    if len(segs) >= 2:
        sub = segs[1].lower()
        if sub == "tagged":
            return Target(
                TargetType.TAGGED_FEED, username, original,
                f"https://www.instagram.com/{username}/tagged/",
            )
        if sub in ("reels", "reel"):
            return Target(
                TargetType.REELS_FEED, username, original,
                f"https://www.instagram.com/{username}/reels/",
            )

    return Target(TargetType.PROFILE, username, original, profile_url(username))


def parse_targets(raws: list[str], *, strict: bool = False) -> tuple[list[Target], list[tuple[str, str]]]:
    """Parse many URLs.

    Returns ``(targets, failures)`` where each failure is ``(url, reason)``.
    With ``strict=True`` the first bad URL raises instead.
    """
    targets: list[Target] = []
    failures: list[tuple[str, str]] = []
    for raw in raws:
        try:
            targets.append(parse_target(raw))
        except UnsupportedUrlError as exc:
            if strict:
                raise
            failures.append((raw, str(exc)))
    return targets, failures


def post_url(shortcode: str, kind: str = "p") -> str:
    """Canonical permalink for a media shortcode."""
    prefix = "reel" if kind in ("reel", "reels") else ("tv" if kind == "tv" else "p")
    return f"https://www.instagram.com/{prefix}/{shortcode}/"


def profile_url(username: str) -> str:
    """Canonical profile URL."""
    return f"https://www.instagram.com/{username}/"


def resolve_user_id(target: Target, api: Any) -> Target:
    """A profile target given by numeric id, re-keyed by its username.

    Any other target is returned unchanged. ``api`` is an
    :class:`~instagram_scraper.ig.api.InstagramApi`; the lookup is one
    request (``/api/v1/users/{id}/info/``) and raises ``NotFoundError`` for
    an unknown id.
    """
    user_id = target.extra.get("user_id") if target.type == TargetType.PROFILE else None
    if not user_id or not str(target.key).isdigit():
        # already keyed by a username (a search hit that also knows its id)
        return target
    username = api.username_for(user_id)
    return Target(TargetType.PROFILE, username, target.input_url, profile_url(username),
                  {**target.extra, "username": username})
