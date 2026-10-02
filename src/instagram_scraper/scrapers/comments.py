"""`resultsType: comments` -- one record per comment on a post or reel."""

from __future__ import annotations

import logging
from dataclasses import replace
from typing import Any, Iterator

from ..errors import (
    ExtractionError,
    FatalError,
    InstagramError,
    LoginRequiredError,
    NotFoundError,
)
from ..mappers.comment import map_comment
from ..ig.network import NetworkCapture
from ..shortcode import shortcode_to_media_id
from ..urls import Target, TargetType, post_url
from .base import BaseScraper

log = logging.getLogger(__name__)


class CommentsScraper(BaseScraper):
    """Comments for media targets; profile targets fan out to their posts."""

    name = "comments"

    def run(self, target: Target) -> Iterator[dict[str, Any]]:
        self._require_session()

        if target.type.is_media:
            yield from self.limited(self._for_media(target), self.config.results_limit)
            return

        if target.type in (TargetType.PROFILE, TargetType.HASHTAG, TargetType.PLACE):
            yield from self._fan_out(target)
            return

        raise InstagramError(
            f"comments can only be scraped from post URLs; got {target.type.value}"
        )

    def _require_session(self) -> None:
        """Comments are invisible to a logged-out viewer, fallback or not.

        Instagram renders the caption and a sign-up prompt on an anonymous post
        page but omits the comment list entirely, so the vision model has
        nothing to read.  Failing here keeps a run from paying for an
        extraction pass that cannot succeed.
        """
        if not self.scrape.authenticated:
            raise LoginRequiredError(
                "comments. Instagram hides the comment list from logged-out "
                "visitors entirely, so supply `sessionCookies` (an Instagram "
                "sessionid) -- the vision fallback cannot substitute for it"
            )

    # ---------------------------------------------------------- single post --

    def _for_media(self, target: Target) -> Iterator[dict[str, Any]]:
        shortcode = target.key
        permalink = post_url(shortcode)
        emitted = 0

        try:
            media_id = shortcode_to_media_id(shortcode)
            captured = None
            if self.config.capture_network_comments:
                self.ctx.goto(permalink, wait=max(3.0, self.config.grid_scroll_pause),
                              refresh_tokens=True)
                if not self.ctx.authenticated:
                    raise LoginRequiredError("the post comment page")
                captured = NetworkCapture(media_id=str(media_id),
                                          newest_first=self.config.is_newest_comments)
                captured.poll(self.ctx.session)
            for raw in self.api.iter_comments(
                media_id,
                max_items=self.config.results_limit,
                newest_first=self.config.is_newest_comments,
                include_replies=self.config.include_nested_comments,
                initial_comments=list(captured.comments.values()) if captured else None,
                first_page=captured.first_comment_page if captured else None,
                reply_pages=captured.reply_pages if captured else None,
                max_replies_per_comment=self.config.max_replies_per_comment,
            ):
                record = map_comment(raw, post_url=permalink, input_url=target.input_url)
                if not self.within_dates(record):
                    # Comment threads are ranked, not chronological, so an
                    # out-of-range comment is skipped rather than a stop signal.
                    self.stats.date_filtered += 1
                    continue
                emitted += 1
                yield record
            # A valid empty list is a successful empty post, not a reason to
            # ask a vision model to invent/find a comment list.
            return
        except NotFoundError:
            raise
        except LoginRequiredError as exc:
            if self.config.session_cookies:
                raise FatalError("Instagram rejected authenticated comment access; "
                                 "stopping without a vision fallback") from exc
            raise
        except InstagramError as exc:
            if emitted:
                log.warning("[comments] %s stopped after %d items: %s",
                            shortcode, emitted, exc)
                raise  # runner preserves rows and records the target failure
            if not self.should_fall_back(exc):
                raise
            self.note_fallback(f"comments:{type(exc).__name__}")

        if not self.config.ai_fallback:
            raise ExtractionError(
                f"comments on {shortcode} need an authenticated session and "
                "`aiFallback` is disabled"
            )

        rows = self.ai.comments(permalink, limit=self.config.results_limit)
        self.stats.ai_calls = self.ai.calls
        for index, row in enumerate(rows):
            yield {
                "inputUrl": target.input_url,
                "id": None,
                "postUrl": permalink,
                "commentUrl": None,
                "text": row.get("text"),
                "ownerUsername": row.get("ownerUsername"),
                "ownerProfilePicUrl": None,
                "timestamp": None,
                "timestampText": row.get("timestampText"),
                "repliesCount": row.get("repliesCount"),
                "replies": [],
                "likesCount": row.get("likesCount"),
                "owner": None,
                "position": index,
                "dataSource": "ai",
            }

    # ------------------------------------------------------------- fan-out ---

    def _fan_out(self, target: Target) -> Iterator[dict[str, Any]]:
        """Collect posts for a feed target, then scrape each one's comments.

        `resultsLimit` applies per post, matching the upstream actor.
        """
        from .posts import PostsScraper

        discovery = self.scrape
        if self.config.comment_post_limit is not None:
            discovery = replace(self.scrape, config=replace(
                self.config, results_limit=self.config.comment_post_limit))
        posts_scraper = PostsScraper(discovery)
        posts = list(posts_scraper.run(target))
        log.info("[comments] fanning out over %d posts from %s", len(posts), target)

        for post in posts:
            shortcode = post.get("shortCode")
            if not shortcode:
                continue
            sub_target = Target(
                TargetType.POST, shortcode, target.input_url, post_url(shortcode)
            )
            try:
                for record in self.limited(
                    self._for_media(sub_target), self.config.results_limit
                ):
                    record["postOwnerUsername"] = post.get("ownerUsername")
                    record["postShortCode"] = shortcode
                    yield record
            except InstagramError as exc:
                log.warning("[comments] skipping %s: %s", shortcode, exc)
                self.stats.errors[type(exc).__name__] += 1
                self.stats.pagination.append({"kind": "comments", "postShortCode": shortcode,
                                              "stopReason": "error", "issues": ["post_failed"],
                                              "error": type(exc).__name__})
