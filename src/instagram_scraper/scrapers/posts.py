"""`resultsType: posts` (and `reels`).

Resolution chain per target:

profile
    ``/api/v1/feed/user/{id}/`` -> the timeline edges already inside
    ``web_profile_info`` -> AI scraping of the rendered grid.

hashtag / place
    ``/api/v1/{tags,locations}/.../sections/`` (needs auth) -> AI scraping.

single post / reel
    ``/api/v1/media/{pk}/info/`` (needs auth) -> the embed renderer -> AI.
"""

from __future__ import annotations

import logging
import random
import time
from typing import Any, Iterator

from ..errors import EndpointNotServedError, ExtractionError, InstagramError, NotFoundError
from ..ig import embed as embed_mod
from ..ig import grid
from ..mappers.post import map_post
from ..shortcode import shortcode_to_media_id
from ..urls import Target, TargetType
from .base import BaseScraper

log = logging.getLogger(__name__)


class PostsScraper(BaseScraper):
    """Yields post records for any target that can produce them."""

    name = "posts"

    #: when True, non-reel media are dropped (drives `resultsType: reels`)
    reels_only = False

    def _map_post(self, raw, **kwargs):
        record = map_post(raw, preserve_unknowns=self.config.collection_mode == "complete", **kwargs)
        if self.config.collection_mode == "complete" and raw.get("_observed_at") is not None:
            record["_observed_at"] = raw["_observed_at"]
        return record

    def run(self, target: Target) -> Iterator[dict[str, Any]]:
        limit = self.config.results_limit

        if target.type in (TargetType.PROFILE, TargetType.REELS_FEED):
            yield from self.limited(self._profile_posts(target), limit)
        elif target.type is TargetType.TAGGED_FEED:
            yield from self.limited(self._tagged_posts(target), limit)
        elif target.type is TargetType.HASHTAG:
            yield from self.limited(self._hashtag_posts(target), limit)
        elif target.type is TargetType.PLACE:
            yield from self.limited(self._place_posts(target), limit)
        elif target.type.is_media:
            yield from self.limited(self._single_post(target), limit)
        else:
            raise InstagramError(
                f"{target.type.value} targets do not yield posts; "
                "use resultsType 'details' or 'stories'"
            )

    # ------------------------------------------------------------- profiles --

    def _profile_posts(self, target: Target) -> Iterator[dict[str, Any]]:
        username = target.key
        want_reels = self.reels_only or target.type is TargetType.REELS_FEED

        try:
            profile = self.scrape.profile(username, user_id=(target.extra or {}).get("user_id"))
        except NotFoundError:
            raise
        except InstagramError as exc:
            if not self.should_fall_back(exc):
                raise
            self.note_fallback(f"profile-lookup:{type(exc).__name__}")
            # Losing the profile lookup does not cost us the grid: it needs only the
            # rendered page and the media API. Try that before paying for a
            # vision pass, which is slower and returns far thinner records.
            if self.scrape.authenticated:
                produced = 0
                for record in self._grid_posts(
                    target, None, skip=set(), want=self.config.results_limit
                ):
                    produced += 1
                    yield record
                    if produced >= self.config.results_limit:
                        return
                if produced:
                    return
            yield from self._ai_grid(target, "profile")
            return

        self.api.ensure_visible(profile)
        parent = self._parent_block(profile) if self.config.add_parent_data else None
        user_id = str(profile.get("id"))

        limit = self.config.results_limit
        emitted = 0
        seen: set[str] = set()
        floor = self.date_floor()
        feed_error: InstagramError | None = None
        hit_floor = False

        def keep(record: dict[str, Any], raw: dict[str, Any] | None = None) -> bool:
            """Shared filtering, so every source applies the same rules."""
            code = record.get("shortCode")
            if code and code in seen:
                return False
            if want_reels and raw is not None and not _is_reel(record, raw):
                return False
            if self.config.skip_pinned_posts and record.get("isPinned"):
                return False
            if not self.within_dates(record):
                return False
            if code:
                seen.add(code)
            return True

        # A logged-in web session is not served the REST listings (they answer
        # with the HTML app page, 2026-09-24); the page's own grid -- reached
        # below through the profile timeline and the grid harvest -- is how
        # the web client itself reads them.
        web_session = bool(getattr(getattr(self.ctx, "tokens", None), "web_confirmed", False))
        feeds = [] if web_session else [self.api.iter_user_feed]
        if want_reels and not web_session and (self.scrape.authenticated or getattr(self.ctx, "cookies", None)):
            # The Reels tab needs the account cookies: without them it refused
            # (2026-09-24), with cookies whose web login had lapsed (the
            # "Continue" screen, accepted only under requireValidSession:
            # false) it still answered and paginated.
            feeds.insert(0, self.api.iter_user_clips)
        for fetch in feeds:
            try:
                for raw in fetch(user_id, max_items=self._fetch_budget()):
                    record = self._map_post(raw, input_url=target.input_url, parent=parent)
                    if floor.passed(record):
                        hit_floor = True
                        break
                    if not keep(record, raw):
                        continue
                    emitted += 1
                    yield record
                    if emitted >= limit:
                        return
            except InstagramError as exc:
                if self.fatal_for_target(exc):
                    raise
                feed_error = exc
                log.info("[posts] @%s feed yielded %d item(s) before %s",
                         username, emitted, type(exc).__name__)
                self.note_fallback(f"feed:{type(exc).__name__}")
            if hit_floor:
                break

        if hit_floor:
            return

        # `web_profile_info` already carried the first page of the timeline, so
        # reading it costs nothing extra.  It therefore runs even when the
        # vision fallback is switched off -- gating a free recovery behind
        # `aiFallback` would throw away data we have already paid for.
        if emitted < limit:
            for record in self._timeline_from_profile(profile, target, parent):
                if not keep(record):
                    continue
                emitted += 1
                yield record
                if emitted >= limit:
                    return

        # Instagram will not serve a 13th post to any single call. With a
        # session, though, the grid itself keeps loading on scroll, so harvest
        # it and rebuild full records from the media API.
        if emitted < limit and self.scrape.authenticated:
            for record in self._grid_posts(target, parent, skip=set(seen),
                                           want=limit - emitted):
                if not keep(record):
                    continue
                emitted += 1
                yield record
                if emitted >= limit:
                    return

        if emitted:
            return

        if feed_error is not None and not self.should_fall_back(feed_error):
            raise feed_error
        yield from self._ai_grid(target, "profile")

    def _grid_posts(
        self,
        target: Target,
        parent: dict[str, Any] | None,
        *,
        skip: set[str],
        want: int,
    ) -> Iterator[dict[str, Any]]:
        """Reuse complete grid responses, looking up only sparse/missing media."""
        want_reels = self.reels_only or target.type is TargetType.REELS_FEED
        # Over-harvest a little: some codes are already in the timeline page,
        # and reel filtering discards more -- except on the Reels tab itself,
        # where every tile is a reel (and the logged-in tab loads three per
        # scroll, so tripling the target tripled the scrolling).
        factor = 3 if (want_reels and target.type is not TargetType.REELS_FEED) else 1
        budget = (want + len(skip)) * factor + 12

        if self.config.collection_mode == "complete":
            for batch in grid.iter_grid(self.ctx, target.url, want=budget,
                                        pause=self.config.grid_scroll_pause,
                                        max_scrolls=self.config.grid_max_scrolls,
                                        capture_network=self.config.capture_network_responses):
                for code in batch.shortcodes:
                    if code in skip:
                        continue
                    raw = batch.media.get(code)
                    if raw:
                        record = self._map_post(raw, input_url=target.input_url, parent=parent, data_source="network")
                        if want_reels and not _is_reel(record, raw):
                            continue
                    else:
                        # Save identity immediately. The complete-mode queue
                        # enriches it without delaying discovery of later tiles.
                        record = {"id": str(shortcode_to_media_id(code)), "shortCode": code,
                                  "url": f"https://www.instagram.com/p/{code}/",
                                  "inputUrl": target.input_url, "dataSource": "grid"}
                    yield record
            return

        try:
            harvest = grid.collect_shortcodes(
                self.ctx, target.url, want=budget,
                pause=self.config.grid_scroll_pause,
                max_scrolls=self.config.grid_max_scrolls,
                capture_network=self.config.capture_network_responses,
            )
        except InstagramError as exc:
            log.warning("[%s] grid harvest failed for %s: %s", self.name, target, exc)
            return

        codes = [c for c in harvest.shortcodes if c not in skip]
        if not codes:
            return
        log.info("[%s] grid gave %d new shortcode(s), %d with complete network JSON",
                 self.name, len(codes), len(harvest.media))

        # Filtering is the caller's job, so counting stays in one place.
        produced = 0
        for index, code in enumerate(codes):
            cached = harvest.media.get(code)
            try:
                raw = cached if cached is not None else self.api.media_by_shortcode(code)
            except NotFoundError:
                continue
            except InstagramError as exc:
                # Throttling here is expected on long harvests: stop cleanly
                # and keep whatever was already rebuilt.
                log.warning("[%s] media lookup stopped after %d post(s): %s: %s",
                            self.name, produced, type(exc).__name__, exc)
                return

            record = self._map_post(raw, input_url=target.input_url, parent=parent,
                              data_source="network" if cached is not None else None)
            if not want_reels or _is_reel(record, raw):
                produced += 1
                yield record

            # Pace the lookups, with jitter. Rebuilding a grid post-by-post is
            # far more request-heavy than the browsing it imitates, and running
            # it flat out is what gets a session throttled and an account
            # flagged.
            pause = self.config.media_lookup_pause
            if pause and cached is None and index + 1 < len(codes):
                time.sleep(pause * random.uniform(0.7, 1.4))

    def _timeline_from_profile(
        self, profile: dict[str, Any], target: Target, parent: dict[str, Any] | None
    ) -> Iterator[dict[str, Any]]:
        edges = ((profile.get("edge_owner_to_timeline_media") or {}).get("edges")) or []
        for edge in edges:
            node = (edge or {}).get("node")
            if not isinstance(node, dict):
                continue
            record = self._map_post(node, input_url=target.input_url, parent=parent)
            if (self.reels_only or target.type is TargetType.REELS_FEED) and not _is_reel(record, node):
                continue
            if self.within_dates(record):
                yield record

    def _tagged_posts(self, target: Target) -> Iterator[dict[str, Any]]:
        """Posts this profile was tagged in -- the `mentions` feed by URL.

        A confirmed web session reads the Tagged tab itself: Instagram answers
        its ``usertags`` feed with 429 and a page (2026-09-29)."""
        if self.scrape.web_session:
            yield from self._tagged_tab_posts(target)
            return
        emitted = 0
        try:
            user_id = self.scrape.user_id(target.key)
            for raw in self.api.iter_tagged_feed(user_id, max_items=self._fetch_budget()):
                record = self._map_post(raw, input_url=target.input_url)
                if self.within_dates(record):
                    emitted += 1
                    yield record
            return
        except EndpointNotServedError as exc:
            if emitted:
                return
            self.note_fallback(f"tagged:{type(exc).__name__}")
            yield from self._tagged_tab_posts(target)
            return
        except InstagramError as exc:
            if self.fatal_for_target(exc) or not self.should_fall_back(exc):
                raise
            self.note_fallback(f"tagged:{type(exc).__name__}")
        yield from self._ai_grid(target, "tagged posts")

    def _tagged_tab_posts(self, target: Target) -> Iterator[dict[str, Any]]:
        """The Tagged tab's tiles, completed through ``media/{pk}/info``."""
        for record in self._grid_posts(target, None, skip=set(), want=self._fetch_budget()):
            if self.within_dates(record):
                yield record

    # ------------------------------------------------------- tags & places ---

    def _hashtag_posts(self, target: Target) -> Iterator[dict[str, Any]]:
        parent = {"type": "hashtag", "name": target.key} if self.config.add_parent_data else None
        emitted = 0
        try:
            for raw in self.api.iter_hashtag_media(target.key, max_items=self._fetch_budget()):
                record = self._map_post(raw, input_url=target.input_url, parent=parent)
                if self.within_dates(record):
                    emitted += 1
                    yield record
        except InstagramError as exc:
            if emitted or not self.should_fall_back(exc):
                if emitted:
                    return
                raise
            self.note_fallback(f"hashtag:{type(exc).__name__}")
        if not emitted:
            yield from self._ai_grid(target, "hashtag", parent=parent)

    def _place_posts(self, target: Target) -> Iterator[dict[str, Any]]:
        parent = ({"type": "place", "id": target.key, "name": target.extra.get("slug")}
                  if self.config.add_parent_data else None)
        emitted = 0
        try:
            for raw in self.api.iter_location_media(target.key, max_items=self._fetch_budget()):
                record = self._map_post(raw, input_url=target.input_url, parent=parent)
                if self.within_dates(record):
                    emitted += 1
                    yield record
        except InstagramError as exc:
            if emitted:
                return
            if not self.should_fall_back(exc):
                raise
            self.note_fallback(f"place:{type(exc).__name__}")
        if not emitted:
            yield from self._ai_grid(target, "place", parent=parent)

    # ---------------------------------------------------------- single post --

    def _single_post(self, target: Target) -> Iterator[dict[str, Any]]:
        shortcode = target.key
        try:
            raw = self.api.media_by_shortcode(shortcode)
            yield self._map_post(raw, input_url=target.input_url)
            return
        except NotFoundError:
            raise
        except InstagramError as exc:
            if not self.config.ai_fallback and not isinstance(exc, InstagramError):
                raise
            self.note_fallback(f"media-info:{type(exc).__name__}")

        # Anonymous fallback #1: the embed renderer.
        try:
            data = embed_mod.fetch_embed(self.ctx, shortcode)
            if not embed_mod.looks_empty(data):
                record = _record_from_embed(data, target)
                if self.within_dates(record):
                    yield record
                return
        except NotFoundError:
            raise
        except InstagramError as exc:
            log.debug("[posts] embed fallback failed for %s: %s", shortcode, exc)

        # Anonymous fallback #2: read the post page with the vision model.
        if not self.config.ai_fallback:
            raise ExtractionError(
                f"could not read post {shortcode}: the private API needs a "
                "session cookie and `aiFallback` is disabled"
            )
        self.note_fallback("post-detail:ai")
        data = self.ai.post_detail(target.url)
        record = _blank_post_record()
        record.update({k: v for k, v in data.items() if v is not None})
        record["inputUrl"] = target.input_url
        record.setdefault("url", target.url)
        yield record

    # ------------------------------------------------------------------ AI ---

    def _ai_grid(
        self, target: Target, label: str, parent: dict[str, Any] | None = None
    ) -> Iterator[dict[str, Any]]:
        if not self.config.ai_fallback:
            raise ExtractionError(
                f"{label} posts need an authenticated session and `aiFallback` "
                "is disabled"
            )
        rows = self.ai.posts_from_page(
            target.url, limit=self.config.results_limit, context_label=label
        )
        self.stats.ai_calls = self.ai.calls
        for row in rows:
            record = _blank_post_record()
            record.update({k: v for k, v in row.items() if v is not None})
            record["inputUrl"] = target.input_url
            if parent:
                record.update(
                    {"fromHashtag": parent.get("name")} if parent.get("type") == "hashtag"
                    else {"fromPlace": parent.get("name")}
                )
            if self.config.enrich_from_embed:
                self._enrich(record)
            if self.within_dates(record):
                yield record

    def _enrich(self, record: dict[str, Any]) -> None:
        """Fill an AI-discovered record from the anonymous embed renderer.

        A logged-out hashtag or place grid only paints the tile and its view
        count, so caption, author, timestamp and media URL are missing.  The
        embed page carries them and is not gated, at the cost of one extra
        page load per post.
        """
        shortcode = record.get("shortCode")
        if not shortcode:
            return
        try:
            data = embed_mod.fetch_embed(self.ctx, shortcode)
        except InstagramError as exc:
            log.debug("[posts] embed enrichment failed for %s: %s", shortcode, exc)
            return

        for source, field in (
            ("caption", "caption"), ("ownerUsername", "ownerUsername"),
            ("ownerFullName", "ownerFullName"), ("displayUrl", "displayUrl"),
            ("videoUrl", "videoUrl"), ("alt", "alt"), ("likesCount", "likesCount"),
            ("dimensionsWidth", "dimensionsWidth"),
            ("dimensionsHeight", "dimensionsHeight"),
        ):
            value = data.get(source)
            if value is not None and record.get(field) in (None, "", []):
                record[field] = value

        if data.get("timestamp") and not record.get("timestamp"):
            from ..mappers.common import iso_timestamp
            record["timestamp"] = iso_timestamp(data["timestamp"])
        if record.get("displayUrl") and not record.get("images"):
            record["images"] = [record["displayUrl"]]
        if record.get("caption"):
            from ..mappers.common import extract_hashtags, extract_mentions
            record["hashtags"] = record.get("hashtags") or extract_hashtags(record["caption"])
            record["mentions"] = record.get("mentions") or extract_mentions(record["caption"])
        if not record.get("id"):
            record["id"] = str(shortcode_to_media_id(shortcode))
        record["dataSource"] = "ai+embed"

    # -------------------------------------------------------------- helpers --

    def _fetch_budget(self) -> int:
        """How many raw items to pull before filtering.

        Date filters and reel filters both discard items, so over-fetch a
        little rather than come up short.
        """
        limit = self.config.results_limit
        if self.config.only_posts_newer_than or self.config.until_date:
            return limit * 4
        if self.reels_only:
            return limit * 3
        return limit + 6

    @staticmethod
    def _parent_block(profile: dict[str, Any]) -> dict[str, Any]:
        return {
            "type": "profile",
            "username": profile.get("username"),
            "id": str(profile.get("id") or ""),
        }


class ReelsScraper(PostsScraper):
    """`resultsType: reels` -- the posts scraper restricted to clips."""

    name = "reels"
    reels_only = True


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #

_REEL_PRODUCT_TYPES = {"clips", "igtv", "feed_video"}


def _is_reel(record: dict[str, Any], raw: dict[str, Any]) -> bool:
    product = str(record.get("productType") or raw.get("product_type") or "").lower()
    if product in _REEL_PRODUCT_TYPES:
        return product != "feed_video" or record.get("type") == "Video"
    return bool(raw.get("clips_metadata")) or str(record.get("type")) == "Video"


def _blank_post_record() -> dict[str, Any]:
    """A post record with every documented key present, so CSV columns line up."""
    return {
        "inputUrl": None, "id": None, "type": None, "shortCode": None,
        "caption": None, "hashtags": [], "mentions": [], "url": None,
        "commentsCount": None, "firstComment": None, "latestComments": [],
        "dimensionsHeight": None, "dimensionsWidth": None, "displayUrl": None,
        "images": [], "videoUrl": None, "alt": None, "likesCount": None,
        "videoViewCount": None, "videoPlayCount": None, "timestamp": None,
        "childPosts": [], "ownerFullName": None, "ownerUsername": None,
        "ownerId": None, "productType": None, "videoDuration": None,
        "isSponsored": None, "isPinned": None, "isCommentsDisabled": None,
        "taggedUsers": [], "coauthorProducers": [], "musicInfo": None,
        "locationName": None, "locationId": None, "dataSource": "ai",
    }


def _record_from_embed(data: dict[str, Any], target: Target) -> dict[str, Any]:
    """Turn embed-page fields into a post record."""
    from ..mappers.common import extract_hashtags, extract_mentions, iso_timestamp

    caption = data.get("caption")
    record = _blank_post_record()
    record.update({
        "inputUrl": target.input_url,
        "id": data.get("mediaIdHint") or (
            str(shortcode_to_media_id(target.key)) if target.key else None
        ),
        "type": "Video" if data.get("isVideo") else "Image",
        "shortCode": data.get("shortCode") or target.key,
        "caption": caption,
        "hashtags": extract_hashtags(caption),
        "mentions": extract_mentions(caption),
        "url": data.get("url") or target.url,
        "displayUrl": data.get("displayUrl"),
        "images": [data["displayUrl"]] if data.get("displayUrl") else [],
        "videoUrl": data.get("videoUrl"),
        "alt": data.get("alt"),
        "likesCount": data.get("likesCount"),
        "dimensionsHeight": data.get("dimensionsHeight"),
        "dimensionsWidth": data.get("dimensionsWidth"),
        "timestamp": iso_timestamp(data.get("timestamp")),
        "ownerUsername": data.get("ownerUsername"),
        "ownerFullName": data.get("ownerFullName"),
        "dataSource": "embed",
    })
    return record
