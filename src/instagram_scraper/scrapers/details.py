"""`resultsType: details` -- profile, hashtag and place metadata records."""

from __future__ import annotations

import logging
import random
import time
from typing import Any, Callable, Iterator

from ..errors import ExtractionError, InstagramError, NotFoundError
from ..ig import grid as page_grid
from ..ig import page
from ..mappers.post import map_post
from ..mappers.profile import (
    map_hashtag,
    map_place,
    map_profile,
    map_profile_with_timeline,
)
from ..urls import Target, TargetType
from .base import BaseScraper

log = logging.getLogger(__name__)

#: how many posts to nest under `latestPosts` in a details record
NESTED_POSTS = 12


class DetailsScraper(BaseScraper):
    """One metadata record per target."""

    name = "details"

    def run(self, target: Target) -> Iterator[dict[str, Any]]:
        if target.type in (TargetType.PROFILE, TargetType.REELS_FEED,
                           TargetType.TAGGED_FEED, TargetType.STORY):
            yield self._profile(target)
        elif target.type is TargetType.HASHTAG:
            yield self._hashtag(target)
        elif target.type is TargetType.PLACE:
            yield self._place(target)
        elif target.type.is_media:
            yield from self._media(target)
        else:
            raise InstagramError(f"cannot produce details for {target.type.value}")
        self.stats.items += 1

    # ------------------------------------------------------------- profiles --

    def _profile(self, target: Target) -> dict[str, Any]:
        username = target.key
        try:
            raw = self.scrape.profile(username, user_id=(target.extra or {}).get("user_id"))
        except NotFoundError:
            raise
        except InstagramError as exc:
            if not self.should_fall_back(exc):
                raise
            self.note_fallback(f"profile:{type(exc).__name__}")

            # The rendered page carries the counts, the display name and the
            # handle in og:description. Reading them costs one `run_js` and is
            # far more accurate than a vision pass, which returned a null
            # username for a page that plainly showed one.
            try:
                from_page = page.read_profile(self.ctx, username)
            except InstagramError as page_exc:
                log.debug("[details] page read failed for @%s: %s",
                          username, page_exc)
                from_page = {}
            if from_page:
                self.stats.fallbacks[f"{self.name}:page-profile"] += 1
                return map_profile(
                    from_page,
                    input_url=target.input_url,
                    latest_posts=self._latest_posts_from_grid(target),
                    add_statistics=self.config.add_profile_statistics,
                )

            if not self.config.ai_fallback:
                raise
            data = self.ai.profile(target.url)
            self.stats.ai_calls = self.ai.calls
            return map_profile(
                _ai_profile_to_raw(data),
                input_url=target.input_url,
                add_statistics=self.config.add_profile_statistics,
            )

        return map_profile_with_timeline(
            raw,
            input_url=target.input_url,
            max_posts=NESTED_POSTS,
            add_statistics=self.config.add_profile_statistics,
        )

    # ------------------------------------------------------------- hashtags --

    def _hashtag(self, target: Target) -> dict[str, Any]:
        try:
            raw = self.api.hashtag(target.key)
        except InstagramError as exc:
            if not self.should_fall_back(exc):
                raise
            self.note_fallback(f"hashtag:{type(exc).__name__}")
            data = self._header_or_ai(
                target, lambda: self.ai.hashtag(target.url, target.key)
            )
            data.setdefault("name", target.key)
            return map_hashtag(data, input_url=target.input_url, name=target.key)

        return map_hashtag(
            raw,
            input_url=target.input_url,
            name=target.key,
            top_posts=_nested_posts(raw, "top"),
            latest_posts=_nested_posts(raw, "recent"),
        )

    # --------------------------------------------------------------- places --

    def _place(self, target: Target) -> dict[str, Any]:
        try:
            raw = self.api.location(target.key)
        except InstagramError as exc:
            if not self.should_fall_back(exc):
                raise
            self.note_fallback(f"place:{type(exc).__name__}")
            data = self._header_or_ai(
                target, lambda: self.ai.place(target.url, target.key)
            )
            data.setdefault("id", target.key)
            return map_place(data, input_url=target.input_url, location_id=target.key)

        return map_place(
            raw,
            input_url=target.input_url,
            location_id=target.key,
            top_posts=_nested_posts(raw, "top"),
            latest_posts=_nested_posts(raw, "recent"),
        )

    def _latest_posts_from_grid(self, target: Target) -> list[dict[str, Any]]:
        """Fill `latestPosts` for a page-sourced profile.

        `web_profile_info` nests the first twelve posts in its response; the
        rendered page does not, so a throttled `details` record would arrive
        without them. The grid carries the same posts, and `media/info`
        rebuilds each in full -- so the fallback record ends up matching the
        API one everywhere except `fbid`, which only the API ever returns.
        """
        if not self.scrape.authenticated:
            return []
        try:
            harvest = page_grid.collect_shortcodes(
                self.ctx, target.url, want=NESTED_POSTS,
                pause=self.config.grid_scroll_pause,
                max_scrolls=max(4, NESTED_POSTS // 4),
                capture_network=self.config.capture_network_responses,
            )
        except InstagramError as exc:
            log.debug("[details] grid harvest for latestPosts failed: %s", exc)
            return []

        posts: list[dict[str, Any]] = []
        for index, code in enumerate(harvest.shortcodes[:NESTED_POSTS]):
            cached = harvest.media.get(code)
            try:
                raw = cached if cached is not None else self.api.media_by_shortcode(code)
            except InstagramError as exc:
                log.debug("[details] latestPosts stopped after %d: %s",
                          len(posts), exc)
                break
            posts.append(map_post(raw, include_children=False,
                                  data_source="network" if cached is not None else None))
            if self.config.media_lookup_pause and cached is None and index + 1 < NESTED_POSTS:
                time.sleep(self.config.media_lookup_pause
                           * random.uniform(0.7, 1.4))
        if posts:
            log.info("[details] filled latestPosts with %d post(s) from the grid",
                     len(posts))
        return posts

    # -------------------------------------------------------------- headers --

    def _header_or_ai(
        self, target: Target, ai_call: Callable[[], dict[str, Any]]
    ) -> dict[str, Any]:
        """Read the rendered page header, falling back to the vision model.

        Hashtag and place pages server-render their name and post count for
        logged-out visitors, so a single `run_js` beats an extraction pass on
        both cost and reliability -- during verification the model returned a
        null name for a place whose `<h1>` read "New York, New York".  The AI
        pass still covers pages whose markup does not carry the header.
        """
        try:
            header = page.read_header(self.ctx, target.url)
        except InstagramError as exc:
            log.debug("[details] page header unavailable for %s: %s", target, exc)
        else:
            if header.usable:
                self.stats.fallbacks[f"{self.name}:page-header"] += 1
                log.info("[details] read %s header from the page: %s (%s posts)",
                         target.type.value, header.name, header.posts_count)
                return header.to_dict()

        if not self.config.ai_fallback:
            raise ExtractionError(
                f"{target.type.value} details need an authenticated session; "
                "the page carried no header and `aiFallback` is disabled"
            )
        data = ai_call()
        self.stats.ai_calls = self.ai.calls
        return data

    # ---------------------------------------------------------------- media --

    def _media(self, target: Target) -> Iterator[dict[str, Any]]:
        """A `details` request pointed at a post is just that post's record."""
        from .posts import PostsScraper

        yield from PostsScraper(self.scrape)._single_post(target)


def _nested_posts(raw: dict[str, Any], which: str) -> list[dict[str, Any]]:
    """Pull the top / recent preview grids out of a tag or place record."""
    key = {"top": ("top", "ranked", "edge_hashtag_to_top_posts", "edge_location_to_top_posts"),
           "recent": ("recent", "edge_hashtag_to_media", "edge_location_to_media")}[which]
    for candidate in key:
        block = raw.get(candidate)
        if isinstance(block, dict):
            edges = block.get("edges") or block.get("sections")
            if isinstance(edges, list) and edges and "node" in (edges[0] or {}):
                return [
                    map_post(edge["node"], include_children=False)
                    for edge in edges[:NESTED_POSTS]
                    if isinstance(edge.get("node"), dict)
                ]
            medias = _section_medias(block.get("sections"))
            if medias:
                return [map_post(media, include_children=False) for media in medias[:NESTED_POSTS]]
    return []


def _section_medias(sections: Any) -> list[dict[str, Any]]:
    """Media records out of layout sections (the 2026-09-22 location shape:
    ``layout_content.medias[].media`` and ``one_by_two_item.clips.items[].media``)."""
    found: list[dict[str, Any]] = []
    for section in sections or []:
        content = (section or {}).get("layout_content") or {}
        items = list(content.get("medias") or []) + list(content.get("fill_items") or [])
        clips = ((content.get("one_by_two_item") or {}).get("clips") or {}).get("items") or []
        for item in items + list(clips):
            media = (item or {}).get("media")
            if isinstance(media, dict) and (media.get("pk") or media.get("id")):
                found.append(media)
    return found


def _ai_profile_to_raw(data: dict[str, Any]) -> dict[str, Any]:
    """Shape an AI-extracted profile like the `web_profile_info` object."""
    return {
        "username": data.get("username"),
        "full_name": data.get("fullName"),
        "biography": data.get("biography"),
        "external_url": data.get("externalUrl"),
        "is_private": data.get("private"),
        "is_verified": data.get("verified"),
        "profile_pic_url": data.get("profilePicUrl"),
        "edge_followed_by": {"count": data.get("followersCount")},
        "edge_follow": {"count": data.get("followsCount")},
        "edge_owner_to_timeline_media": {"count": data.get("postsCount")},
        "dataSource": "ai",
    }
