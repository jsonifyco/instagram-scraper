"""`resultsType: mentions` -- posts where a profile is tagged or @-mentioned.

Sources, in order:

* for a confirmed web session, the profile's *Tagged* tab as the web app
  loads it (``PolarisProfileTaggedTabContentQuery``): Instagram answers such
  a session's ``/api/v1/usertags/{id}/feed/`` with 429 and its "Page Not
  Found" page (2026-09-29), and the web app never calls it;
* otherwise the tagged feed (``/api/v1/usertags/{id}/feed/``), which is what
  the upstream actor reports for a profile URL;
* the vision model on the tagged page, when `aiFallback` allows it.
"""

from __future__ import annotations

import logging
from typing import Any, Iterator

from ..errors import (
    EndpointNotServedError,
    ExtractionError,
    InstagramError,
    LoginRequiredError,
    NotFoundError,
)
from ..ig import endpoints as ep
from ..mappers.post import map_post
from ..urls import Target, TargetType
from .base import BaseScraper

log = logging.getLogger(__name__)


class MentionsScraper(BaseScraper):
    """Yields post records in which the target profile appears."""

    name = "mentions"

    def run(self, target: Target) -> Iterator[dict[str, Any]]:
        if target.type not in (TargetType.PROFILE, TargetType.TAGGED_FEED,
                               TargetType.REELS_FEED, TargetType.STORY):
            raise InstagramError(
                f"mentions need a profile URL; got {target.type.value}"
            )
        if not self.scrape.authenticated:
            # `/<user>/tagged/` redirects a logged-out visitor to
            # /accounts/login/, so the page carries no posts for the vision
            # model to read.  Refuse before paying for an empty pass.
            raise LoginRequiredError(
                f"the tagged feed of @{target.key}. Instagram redirects "
                "/tagged/ to the login screen for logged-out visitors, so "
                "supply `sessionCookies` -- the vision fallback cannot help here"
            )
        yield from self.limited(self._mentions(target), self.config.results_limit)

    def _mentions(self, target: Target) -> Iterator[dict[str, Any]]:
        username = target.key
        emitted = 0

        if self.scrape.web_session:
            # Not a fallback: this is how the web client reads the tab, and
            # the REST feed would only answer 429 with a page.
            yield from self._tagged_tab(target, username)
            return

        floor = self.date_floor()
        try:
            user_id = self.scrape.user_id(username, known_id=(target.extra or {}).get("user_id"))
            for raw in self.api.iter_tagged_feed(
                user_id, max_items=self.config.results_limit * 2
            ):
                record = map_post(raw, input_url=target.input_url)
                record["mentionedProfile"] = username
                if floor.passed(record):
                    return
                if self.within_dates(record):
                    emitted += 1
                    yield record
            if emitted:
                return
        except NotFoundError:
            raise
        except EndpointNotServedError as exc:
            if emitted:
                return
            self.note_fallback(f"tagged-feed:{type(exc).__name__}")
            yield from self._tagged_tab(target, username)
            return
        except InstagramError as exc:
            if emitted:
                return
            if not self.should_fall_back(exc):
                raise
            self.note_fallback(f"tagged-feed:{type(exc).__name__}")

        if not self.config.ai_fallback:
            raise ExtractionError(
                f"the tagged feed of @{username} needs an authenticated "
                "session and `aiFallback` is disabled"
            )

        tagged_url = f"{ep.profile_page(username)}tagged/"
        rows = self.ai.posts_from_page(
            tagged_url, limit=self.config.results_limit,
            context_label=f"tagged-posts of @{username}",
        )
        self.stats.ai_calls = self.ai.calls
        for row in rows:
            record = dict(row)
            record["inputUrl"] = target.input_url
            record["mentionedProfile"] = username
            if self.within_dates(record):
                yield record

    def _tagged_tab(self, target: Target, username: str) -> Iterator[dict[str, Any]]:
        """The Tagged tab of a logged-in web session, tile by tile.

        The tab's own responses carry sparse tiles (no ``taken_at``, no video
        URL; live 2026-09-29), so each post is completed through
        ``media/{pk}/info`` -- which a web session is still served -- paced
        like the Reels tab. The tab's order is the tagged feed's order.
        """
        from .posts import PostsScraper

        tab = Target(TargetType.TAGGED_FEED, username, target.input_url,
                     f"{ep.profile_page(username)}tagged/", dict(target.extra or {}))
        log.info("[mentions] reading @%s's Tagged tab from the page", username)
        floor = self.date_floor()
        posts = PostsScraper(self.scrape)
        for record in posts._grid_posts(tab, None, skip=set(), want=posts._fetch_budget()):
            record["mentionedProfile"] = username
            if floor.passed(record):
                return
            if self.within_dates(record):
                yield record
