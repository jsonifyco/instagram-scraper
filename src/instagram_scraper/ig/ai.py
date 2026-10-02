"""Vision-based fallback built on getbro's autopilot commands.

When an endpoint is gated but the corresponding page remains public, its
rendered content can supply a fallback. Availability varies by page and
Instagram session. This module drives getbro's `extract` (and, where scrolling
is needed, `act`) over those pages.

The output is normalised into the same loose shape the mappers expect, so a
record sourced this way flows through the rest of the pipeline unchanged; it is
tagged with ``dataSource: "ai"`` so consumers can tell the two apart.
"""

from __future__ import annotations

import logging
import re
from typing import Any

from ..bro import commands as cmd
from ..errors import ExtractionError
from .context import IgContext

log = logging.getLogger(__name__)

# --------------------------------------------------------------------------- #
# Schemas handed to the extractor
# --------------------------------------------------------------------------- #

POST_LIST_SCHEMA = [{
    "url": "<full instagram permalink of the post, e.g. https://www.instagram.com/p/ABC123/>",
    "shortCode": "<the shortcode segment of the permalink>",
    "type": "<'Image', 'Video' or 'Sidecar'>",
    "caption": "<post caption text, or null if not shown>",
    "ownerUsername": "<username of the author without the @, or null>",
    "likesCount": "<integer number of likes, or null if not shown>",
    "commentsCount": "<integer number of comments, or null if not shown>",
    "videoViewCount": "<integer view count for videos, or null>",
}]

POST_DETAIL_SCHEMA = {
    "shortCode": "<shortcode from the URL>",
    "type": "<'Image', 'Video' or 'Sidecar'>",
    "caption": "<full caption text>",
    "ownerUsername": "<author username without the @>",
    "ownerFullName": "<author display name, or null>",
    "likesCount": "<integer likes, or null>",
    "commentsCount": "<integer comments, or null>",
    "videoViewCount": "<integer views for a video, or null>",
    "displayUrl": "<direct URL of the main image>",
    "timestamp": "<publication date as shown, or null>",
    "locationName": "<tagged place name, or null>",
}

COMMENT_LIST_SCHEMA = [{
    "ownerUsername": "<commenter username without the @>",
    "text": "<the comment body>",
    "likesCount": "<integer likes on the comment, or null>",
    "timestamp": "<relative or absolute time shown next to the comment, or null>",
    "repliesCount": "<integer number of replies, or null>",
}]

PROFILE_SCHEMA = {
    "username": "<handle without the @>",
    "fullName": "<display name>",
    "biography": "<bio text>",
    "followersCount": "<integer follower count, expand 1.2M to 1200000>",
    "followsCount": "<integer following count>",
    "postsCount": "<integer number of posts>",
    "verified": "<true or false>",
    "private": "<true or false>",
    "externalUrl": "<link in bio, or null>",
    "profilePicUrl": "<URL of the avatar image>",
}

HASHTAG_SCHEMA = {
    "name": "<the hashtag without the # sign>",
    "postsCount": "<integer number of posts, expand 124M to 124000000>",
}

PLACE_SCHEMA = {
    "name": "<name of the place>",
    "postsCount": "<integer number of posts, or null>",
    "address": "<street address, or null>",
}

SEARCH_SCHEMA = [{
    "type": "<'user', 'hashtag' or 'place'>",
    "name": "<username, hashtag name or place name>",
    "url": "<full instagram URL of the entry>",
    "subtitle": "<the secondary line shown, or null>",
}]

_SHORTCODE_RE = re.compile(r"/(?:p|reel|reels|tv)/([A-Za-z0-9_-]+)")
#: The vision model answers with things like "11.7M views" or "1,234 likes",
#: so match the number-plus-suffix anywhere rather than anchoring the string.
_COUNT_RE = re.compile(r"(\d[\d.,]*)\s*([KkMmBb])?(?![\d.,])")


class AiExtractor:
    """Page-scraping fallback bound to one browser context."""

    def __init__(self, ctx: IgContext, *, model_size: str = "small",
                 enabled: bool = True) -> None:
        self.ctx = ctx
        self.model_size = model_size
        self.enabled = enabled
        #: rough accounting so the run summary can show what AI cost
        self.calls = 0
        self.tokens = 0

    # ------------------------------------------------------------- plumbing --

    def _extract(
        self,
        instruction: str,
        schema: Any,
        *,
        viewport_max: int | None = None,
        paginate: bool = False,
        pages: int = 1,
        feed_urls: bool = True,
    ) -> Any:
        if not self.enabled:
            raise ExtractionError("AI fallback is disabled (`aiFallback: false`)")
        self.calls += 1
        command = cmd.extract(
            instruction, schema,
            model_size=self.model_size,
            viewport_max=viewport_max,
            feed_urls=feed_urls,
            paginate=paginate or None,
            pages_to_paginate=pages if paginate else None,
        )
        payload = self.ctx.session.run_one(command)
        data = payload or {}
        return data.get("extracted_json")

    def _scroll_and_extract(
        self,
        instruction: str,
        schema: Any,
        *,
        target: int,
        per_screen: int = 12,
        max_scrolls: int = 12,
    ) -> list[dict[str, Any]]:
        """Scroll a lazily-loaded grid, extracting as we go and de-duplicating."""
        collected: dict[str, dict[str, Any]] = {}
        scrolls = 0
        stagnant = 0

        while len(collected) < target and scrolls <= max_scrolls:
            batch = self._extract(instruction, schema, feed_urls=True)
            before = len(collected)
            for row in _as_rows(batch):
                key = _row_key(row)
                if key and key not in collected:
                    collected[key] = row
            if len(collected) == before:
                stagnant += 1
                if stagnant >= 2:
                    break
            else:
                stagnant = 0
            if len(collected) >= target:
                break

            scrolls += 1
            # `scroll_to_viewport` advances a whole screen at a time, which
            # both loads the next batch of lazy tiles and keeps the extractor
            # looking at fresh content instead of re-reading the same rows.
            self.ctx.session.run([
                cmd.scroll_to_viewport(scrolls),
                cmd.sleep(2.0),
            ])
            log.debug("AI grid scroll %d: %d unique rows", scrolls, len(collected))

        return list(collected.values())[:target]

    # ---------------------------------------------------------------- posts --

    def posts_from_page(self, url: str, *, limit: int, context_label: str) -> list[dict[str, Any]]:
        """Scrape a post grid (profile, hashtag or place page)."""
        self.ctx.goto(url, wait=4.0)
        rows = self._scroll_and_extract(
            f"Extract the Instagram posts displayed in the grid on this "
            f"{context_label} page. Include one entry per post tile.",
            POST_LIST_SCHEMA,
            target=limit,
        )
        out = []
        for row in rows:
            normalised = _normalise_post_row(row)
            if normalised:
                out.append(normalised)
        if not out:
            raise ExtractionError(f"AI fallback found no posts on {url}")
        log.info("AI fallback recovered %d posts from %s", len(out), url)
        return out

    def post_detail(self, url: str) -> dict[str, Any]:
        """Scrape a single post page."""
        self.ctx.goto(url, wait=4.0)
        data = self._extract(
            "Extract the details of the Instagram post shown on this page: "
            "author, caption, engagement counts and the main media.",
            POST_DETAIL_SCHEMA,
            viewport_max=3,
        )
        row = _first_dict(data)
        if not row:
            raise ExtractionError(f"AI fallback could not read the post at {url}")
        normalised = _normalise_post_row(row) or {}
        normalised.setdefault("url", url)
        match = _SHORTCODE_RE.search(url)
        if match:
            normalised["shortCode"] = match.group(1)
        return normalised

    # ------------------------------------------------------------- comments --

    def comments(self, post_url: str, *, limit: int) -> list[dict[str, Any]]:
        """Scrape the comment list of a post page."""
        self.ctx.goto(post_url, wait=4.0)
        rows = self._scroll_and_extract(
            "Extract the comments visible under this Instagram post: the "
            "commenter's username, the comment text, its like count and the "
            "time shown. Do not include the post caption itself.",
            COMMENT_LIST_SCHEMA,
            target=limit,
            max_scrolls=max(3, limit // 8),
        )
        out = []
        for row in rows:
            if not isinstance(row, dict) or not row.get("text"):
                continue
            out.append({
                "ownerUsername": _clean_handle(row.get("ownerUsername")),
                "text": str(row["text"]).strip(),
                "likesCount": _to_int(row.get("likesCount")),
                "repliesCount": _to_int(row.get("repliesCount")),
                "timestampText": row.get("timestamp"),
                "dataSource": "ai",
            })
        if not out:
            raise ExtractionError(f"AI fallback found no comments on {post_url}")
        return out

    # -------------------------------------------------------------- details --

    def profile(self, url: str) -> dict[str, Any]:
        self.ctx.goto(url, wait=4.0)
        row = _first_dict(self._extract(
            "Extract this Instagram profile's header information.",
            PROFILE_SCHEMA, viewport_max=2,
        ))
        if not row:
            raise ExtractionError(f"AI fallback could not read the profile at {url}")
        return {
            "username": _clean_handle(row.get("username")),
            "fullName": row.get("fullName"),
            "biography": row.get("biography"),
            "followersCount": _to_int(row.get("followersCount")),
            "followsCount": _to_int(row.get("followsCount")),
            "postsCount": _to_int(row.get("postsCount")),
            "verified": _to_bool(row.get("verified")),
            "private": _to_bool(row.get("private")),
            "externalUrl": row.get("externalUrl"),
            "profilePicUrl": row.get("profilePicUrl"),
            "dataSource": "ai",
        }

    def hashtag(self, url: str, tag: str) -> dict[str, Any]:
        self.ctx.goto(url, wait=4.0)
        row = _first_dict(self._extract(
            f"Extract the header information for the #{tag} hashtag page.",
            HASHTAG_SCHEMA, viewport_max=2,
        )) or {}
        return {
            "name": (row.get("name") or tag).lstrip("#"),
            "postsCount": _to_int(row.get("postsCount")),
            "dataSource": "ai",
        }

    def place(self, url: str, location_id: str) -> dict[str, Any]:
        self.ctx.goto(url, wait=4.0)
        row = _first_dict(self._extract(
            "Extract the header information for this Instagram place page.",
            PLACE_SCHEMA, viewport_max=2,
        )) or {}
        return {
            "id": location_id,
            "name": row.get("name"),
            "postsCount": _to_int(row.get("postsCount")),
            "address": row.get("address"),
            "dataSource": "ai",
        }

    def search(self, query: str, url: str, *, limit: int) -> list[dict[str, Any]]:
        self.ctx.goto(url, wait=4.5)
        rows = _as_rows(self._extract(
            f"Extract the Instagram search results shown for the query "
            f"{query!r}: accounts, hashtags and places.",
            SEARCH_SCHEMA, viewport_max=4,
        ))
        out = []
        for row in rows[:limit]:
            if not isinstance(row, dict) or not row.get("name"):
                continue
            out.append({
                "type": (row.get("type") or "").lower() or None,
                "name": str(row["name"]).lstrip("#@"),
                "url": row.get("url"),
                "subtitle": row.get("subtitle"),
                "dataSource": "ai",
            })
        if not out:
            raise ExtractionError(f"AI fallback found no search results for {query!r}")
        return out


# --------------------------------------------------------------------------- #
# Normalisation helpers
# --------------------------------------------------------------------------- #

def _as_rows(data: Any) -> list[Any]:
    if data is None:
        return []
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for key in ("items", "results", "posts", "data", "comments"):
            if isinstance(data.get(key), list):
                return data[key]
        return [data]
    return []


def _first_dict(data: Any) -> dict[str, Any] | None:
    for row in _as_rows(data):
        if isinstance(row, dict) and row:
            return row
    return None


def _row_key(row: Any) -> str | None:
    """Stable identity for de-duplicating rows across scroll steps."""
    if not isinstance(row, dict):
        return None
    for key in ("shortCode", "url", "permalink"):
        value = row.get(key)
        if value:
            match = _SHORTCODE_RE.search(str(value))
            return match.group(1) if match else str(value)
    text = row.get("text")
    if text:
        return f"{row.get('ownerUsername')}:{str(text)[:80]}"
    return None


def _clean_handle(value: Any) -> str | None:
    if not value:
        return None
    return str(value).strip().lstrip("@").strip("/") or None


def _to_int(value: Any) -> int | None:
    """Accept ``1234``, ``"1,234"``, ``"12.3K"``, ``"1.2M"``."""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    # Normalise the exotic spaces the vision model sometimes emits.
    text = str(value).replace("\u00a0", " ").replace("\u202f", " ")
    match = _COUNT_RE.search(text)
    if not match:
        return None
    number, suffix = match.group(1), (match.group(2) or "").lower()
    multiplier = {"k": 1_000, "m": 1_000_000, "b": 1_000_000_000}.get(suffix, 1)
    # "1,234" uses a thousands separator; "1.2M" uses a decimal point.  A dot
    # only means "decimal" when a magnitude suffix follows it.
    number = number.replace(",", "")
    if multiplier == 1:
        number = number.replace(".", "")
    try:
        return int(float(number) * multiplier)
    except ValueError:
        return None


def _to_bool(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in ("true", "yes", "1"):
        return True
    if text in ("false", "no", "0"):
        return False
    return None


def _normalise_post_row(row: Any) -> dict[str, Any] | None:
    """Shape one AI-extracted post row like a mapped post record."""
    if not isinstance(row, dict):
        return None
    url = row.get("url") or row.get("permalink")
    shortcode = row.get("shortCode")
    if not shortcode and url:
        match = _SHORTCODE_RE.search(str(url))
        shortcode = match.group(1) if match else None
    if not shortcode and not url:
        return None
    if not url and shortcode:
        url = f"https://www.instagram.com/p/{shortcode}/"

    kind = str(row.get("type") or "").strip().lower()
    type_name = {"video": "Video", "sidecar": "Sidecar", "carousel": "Sidecar",
                 "image": "Image", "photo": "Image", "reel": "Video"}.get(kind)

    return {
        "shortCode": shortcode,
        "url": str(url).split("?")[0] if url else None,
        "type": type_name,
        "caption": row.get("caption"),
        "ownerUsername": _clean_handle(row.get("ownerUsername")),
        "ownerFullName": row.get("ownerFullName"),
        "likesCount": _to_int(row.get("likesCount")),
        "commentsCount": _to_int(row.get("commentsCount")),
        "videoViewCount": _to_int(row.get("videoViewCount")),
        "displayUrl": row.get("displayUrl"),
        "locationName": row.get("locationName"),
        "timestampText": row.get("timestamp"),
        "dataSource": "ai",
    }
