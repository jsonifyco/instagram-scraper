"""`resultsType: stories` -- active story items for a profile.

Stories are only served to an authenticated viewer.  There is no anonymous
path: the rendered story page redirects a logged-out visitor to the login
screen, so the AI fallback has nothing to read.  When no session cookie is
available this scraper raises a clear, actionable error rather than emitting
empty records.
"""

from __future__ import annotations

import logging
from typing import Any, Iterator

from ..errors import InstagramError, LoginRequiredError
from ..mappers.common import all_candidates, as_int, as_str, best_candidate, iso_timestamp
from ..urls import Target, TargetType
from .base import BaseScraper

log = logging.getLogger(__name__)


class StoriesScraper(BaseScraper):
    """Yields one record per story frame."""

    name = "stories"

    def run(self, target: Target) -> Iterator[dict[str, Any]]:
        if target.type not in (TargetType.PROFILE, TargetType.STORY,
                               TargetType.REELS_FEED, TargetType.TAGGED_FEED):
            raise InstagramError(
                f"stories need a profile or story URL; got {target.type.value}"
            )
        if not self.scrape.authenticated:
            raise LoginRequiredError(
                f"stories of @{target.key} (Instagram never serves stories to a "
                "logged-out viewer, so no fallback exists)"
            )

        username = target.key
        user_id = self.scrape.user_id(username, known_id=(target.extra or {}).get("user_id"))
        items = self.api.stories(user_id)
        log.info("[stories] @%s has %d active frames", username, len(items))

        for index, raw in enumerate(items[: self.config.results_limit]):
            record = _map_story(raw, username=username, position=index,
                                input_url=target.input_url)
            if self.within_dates(record):
                self.stats.items += 1
                yield record


def _map_story(
    raw: dict[str, Any], *, username: str, position: int, input_url: str | None
) -> dict[str, Any]:
    """Reshape one story frame."""
    owner = raw.get("user") or {}
    media_type = as_int(raw.get("media_type"), 1)
    return {
        "inputUrl": input_url,
        "id": as_str(raw.get("pk") or raw.get("id")),
        "type": "Video" if media_type == 2 else "Image",
        "position": position,
        "ownerUsername": as_str(owner.get("username")) or username,
        "ownerFullName": as_str(owner.get("full_name")),
        "ownerId": as_str(owner.get("pk") or owner.get("id")),
        "timestamp": iso_timestamp(raw.get("taken_at")),
        "expiringAt": iso_timestamp(raw.get("expiring_at")),
        "displayUrl": best_candidate(raw.get("image_versions2")),
        "images": all_candidates(raw.get("image_versions2")),
        "videoUrl": best_candidate(raw.get("video_versions")),
        "videoDuration": raw.get("video_duration"),
        "dimensionsWidth": as_int(raw.get("original_width")),
        "dimensionsHeight": as_int(raw.get("original_height")),
        "storyUrl": f"https://www.instagram.com/stories/{username}/"
                    f"{raw.get('pk') or ''}/".rstrip("/") + "/",
        "mentions": [
            as_str((tag.get("user") or {}).get("username"))
            for tag in raw.get("reel_mentions") or []
            if isinstance(tag, dict)
        ],
        "hashtags": [
            as_str((tag.get("hashtag") or {}).get("name"))
            for tag in raw.get("story_hashtags") or []
            if isinstance(tag, dict)
        ],
        "links": [
            as_str(link.get("webUri") or link.get("web_uri"))
            for link in raw.get("story_cta") or []
            if isinstance(link, dict)
        ],
        "isPaidPartnership": bool(raw.get("is_paid_partnership")),
        "dataSource": "api",
    }
