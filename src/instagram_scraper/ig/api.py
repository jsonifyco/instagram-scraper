"""Typed access to the Instagram endpoints, with cursor-based pagination.

Every method returns *raw* Instagram JSON.  Reshaping into the Apify output
format is the mappers' job, so that the two concerns stay independently
testable.

Which calls work without a `sessionid` cookie (verified against live traffic):

===========================  =============  =========================
call                         logged out     logged in
===========================  =============  =========================
:meth:`profile`              yes            varies by account (429 observed)
:meth:`iter_user_feed`       may reject     yes
:meth:`media_by_id`          no             yes
:meth:`iter_comments`        no             yes
:meth:`iter_tagged_feed`     no             yes
:meth:`hashtag`              no             yes
:meth:`location`             no             yes
:meth:`search`               no             yes
:meth:`stories`              no             yes
===========================  =============  =========================

Gated calls may raise login, challenge or rate-limit errors; scrapers choose
the supported fallback or stop according to the active session policy.
"""

from __future__ import annotations

import logging
import hashlib
import json
import random
import time
from collections import OrderedDict
from typing import Any, Iterator

from ..errors import NotFoundError, PrivateProfileError
from ..shortcode import shortcode_to_media_id, split_media_id
from . import endpoints as ep
from .context import IgContext

log = logging.getLogger(__name__)

#: Observed upper page size for the user feed; still bound our requests here.
MAX_PAGE_SIZE = 33
#: What the web client itself asks the profile timeline for. Measured
#: 2026-09-24: ``count=33`` is answered with 12 items anyway, so asking for
#: more only makes the request look unlike the page's own.
WEB_FEED_PAGE_SIZE = 12


class InstagramApi:
    """Endpoint-level client bound to one :class:`IgContext`."""

    def __init__(self, context: IgContext, *, pause: float = 0.0,
                 pagination_log: list[dict[str, Any]] | None = None,
                 max_comment_pages: int = 100) -> None:
        self.ctx = context
        #: seconds between the repeated calls a threaded harvest makes
        self.pause = pause
        self.pagination_log = pagination_log if pagination_log is not None else []
        self.max_comment_pages = max_comment_pages
        self.media_cache = OrderedDict()
        self.page_store = None

    def _read_page(self, url, *, body=None, **kwargs):
        """Persist successful discovery pages and their cursors for replay.

        Cached pages can be consumed again after an interrupted discovery
        without another Instagram request. Comment queues commit pages directly.
        """
        key = "discovery:" + hashlib.sha256(json.dumps([url, body]).encode()).hexdigest()
        cached = self.page_store.cache_get(key) if self.page_store else None
        if cached is not None:
            return cached
        result = self.ctx.api_get(url, **kwargs) if body is None else self.ctx.api_post(url, body, **kwargs)
        if self.page_store:
            observed = time.time()
            def annotate(value):
                if isinstance(value, dict):
                    for child in tuple(value.values()):
                        annotate(child)
                    if "id" in value or "pk" in value:
                        value["_observed_at"] = observed
                elif isinstance(value, list):
                    for child in value:
                        annotate(child)
            annotate(result)
            self.page_store.cache_set(key, result)
        return result

    # ------------------------------------------------------------- profiles --

    def profile(self, username: str) -> dict[str, Any]:
        """Profile snapshot, sometimes including a short timeline preview.

        Anonymous reads have worked in live probes; some authenticated
        contexts receive 429, so known numeric IDs use ``user_by_id``.
        """
        url = ep.web_profile_info(username)
        body = self._read_page(
            url, referer=ep.profile_page(username), what=f"profile @{username}"
        )
        user = ((body or {}).get("data") or {}).get("user")
        if not user:
            raise NotFoundError(f"profile @{username} not found")
        return user

    def user_by_id(self, user_id: str | int) -> dict[str, Any]:
        """Profile header for a numeric user id; raises ``NotFoundError`` when
        Instagram knows no such account."""
        url = ep.user_info(user_id)
        body = self._read_page(url, referer=f"{ep.BASE}/", what=f"user #{user_id}")
        user = (body or {}).get("user")
        if not user or not user.get("username"):
            raise NotFoundError(f"user id {user_id} not found")
        return user

    def username_for(self, user_id: str | int) -> str:
        """Resolve a numeric user id to its username (one request, cached)."""
        return str(self.user_by_id(user_id)["username"])

    def iter_user_feed(
        self,
        user_id: str | int,
        *,
        page_size: int = WEB_FEED_PAGE_SIZE,
        max_items: int | None = None,
        start_cursor: str | None = None,
    ) -> Iterator[dict[str, Any]]:
        """Yield timeline items for a profile, newest first.

        Works without authentication.  Handles the ``next_max_id`` cursor and
        stops when Instagram says there is nothing more.
        """
        cursor = start_cursor
        seen = 0
        page = 0
        while True:
            page += 1
            url = ep.user_feed(user_id, count=min(page_size, MAX_PAGE_SIZE), max_id=cursor)
            # Through the page's own fetch, as the web client sends it: the
            # getbro ``fetch_json`` of a cursor-paginated listing replayed its
            # first page (clips/user, 2026-09-24; comment heads, 2026-09-16).
            body = self._read_page(url, what=f"feed of user {user_id}",
                                   transport="page", page_headers=True)
            items = list(body.get("items") or [])
            log.debug("feed page %d for %s: %d items (cursor=%s)",
                      page, user_id, len(items), cursor)

            for item in items:
                yield item
                seen += 1
                if max_items is not None and seen >= max_items:
                    return

            cursor = body.get("next_max_id")
            if not items or not body.get("more_available") or not cursor:
                return

    def iter_user_clips(
        self,
        user_id: str | int,
        *,
        page_size: int = 12,
        max_items: int | None = None,
    ) -> Iterator[dict[str, Any]]:
        """Yield reels from a profile's Reels tab (needs the account cookies).

        Read through the page's own fetch: on the same ``max_id`` it returned
        the next page where ``fetch_json`` returned the first page again.
        """
        cursor: str | None = None
        seen = 0
        seen_ids = set()
        cursors = set()
        while True:
            payload: dict[str, Any] = {
                "target_user_id": str(user_id),
                "page_size": min(page_size, 50),
                "include_feed_video": "true",
            }
            if cursor:
                payload["max_id"] = cursor
            body = self._read_page(
                ep.user_clips(user_id), body=ep.form_body(payload),
                what=f"reels of user {user_id}",
                # Measured 2026-09-24 on the same cursor: the page's fetch
                # returned the next 12 reels, ``fetch_json`` the first 12 again.
                transport="page", page_headers=True,
            )
            entries = list(body.get("items") or [])
            fresh = 0
            for entry in entries:
                media = entry.get("media") if isinstance(entry, dict) else None
                item = media or entry
                key = str(item.get("pk") or item.get("id") or item.get("code") or "") if isinstance(item, dict) else ""
                if not key or key in seen_ids:
                    continue
                seen_ids.add(key)
                fresh += 1
                yield item
                seen += 1
                if max_items is not None and seen >= max_items:
                    return
            paging = body.get("paging_info") or {}
            cursor = paging.get("max_id")
            if not entries or not paging.get("more_available") or not cursor:
                return
            if not fresh:
                # Measured 2026-09-24: through ``fetch_json`` this endpoint
                # answered every new cursor with the same first 12 reels and
                # a fresh cursor, and the loop spent the whole request budget
                # (60 reads) on one page. A page with nothing new ends it.
                log.info("reels of user %s: page %d repeated earlier items; stopping "
                         "this source after %d reel(s)", user_id, len(cursors) + 2, seen)
                return
            if cursor in cursors:
                return
            cursors.add(cursor)

    def iter_tagged_feed(
        self,
        user_id: str | int,
        *,
        page_size: int = MAX_PAGE_SIZE,
        max_items: int | None = None,
    ) -> Iterator[dict[str, Any]]:
        """Yield posts in which this profile is tagged (needs authentication)."""
        cursor: str | None = None
        seen = 0
        while True:
            url = ep.user_tagged_feed(
                user_id, count=min(page_size, MAX_PAGE_SIZE), max_id=cursor
            )
            body = self._read_page(url, what=f"tagged feed of user {user_id}")
            items = list(body.get("items") or [])
            for item in items:
                yield item.get("media") if isinstance(item, dict) and "media" in item else item
                seen += 1
                if max_items is not None and seen >= max_items:
                    return
            cursor = body.get("next_max_id")
            if not items or not body.get("more_available") or not cursor:
                return

    # ---------------------------------------------------------------- media --

    def media_by_id(self, media_id: str | int) -> dict[str, Any]:
        """Full media record (needs authentication)."""
        pk = split_media_id(media_id)
        if pk in self.media_cache:
            return self.media_cache[pk]
        body = self.ctx.api_get(ep.media_info(pk), what=f"media {pk}")
        items = body.get("items") or []
        if not items:
            raise NotFoundError(f"media {pk} not found")
        raw = {**items[0], "_observed_at": time.time()}
        self.media_cache[pk] = raw
        if len(self.media_cache) > 128:
            self.media_cache.popitem(last=False)
        return raw

    def media_by_shortcode(self, shortcode: str) -> dict[str, Any]:
        """Full media record from a `/p/<code>/` shortcode."""
        return self.media_by_id(shortcode_to_media_id(shortcode))

    def iter_comments(
        self, media_id: str | int, *, max_items: int | None = None,
        newest_first: bool = False, include_replies: bool = False,
        initial_comments: list[dict[str, Any]] | None = None,
        first_page: dict[str, Any] | None = None,
        reply_pages: dict[str, dict[str, Any]] | None = None,
        max_replies_per_comment: int | None = None,
    ) -> Iterator[dict[str, Any]]:
        """Unique parents and optional replies, sharing one output budget."""
        from .comment_pages import identity, walk
        pk = split_media_id(media_id)
        delivered: set[str] = set()
        report = {"kind": "comments", "mediaId": str(pk), "apiRequests": 0,
                  "networkSeedItems": len(initial_comments or []),
                  "reusedFirstPage": first_page is not None}
        self.pagination_log.append(report)
        def fetch(**params):
            nonlocal first_page
            if not params and first_page is not None:
                body, first_page = first_page, None
                return body
            report["apiRequests"] += 1
            return self.ctx.api_get(ep.media_comments(
                pk, sort_order="recent" if newest_first else None, **params),
                what=f"comments of media {pk}")
        parents = walk(fetch, replies=False, report=report,
                       max_pages=self.max_comment_pages, max_items=max_items,
                       initial=initial_comments)
        try:
            for comment in parents:
                key = identity(comment)
                if key in delivered:
                    continue
                delivered.add(key)
                yield comment
                if max_items is not None and len(delivered) >= max_items:
                    report["stopReason"] = "results_limit"
                    return
                previews = (comment.get("preview_child_comments")
                            or (comment.get("edge_threaded_comments") or {}).get("edges") or [])
                expected = comment.get("child_comment_count")
                if expected is None:
                    expected = (comment.get("edge_threaded_comments") or {}).get("count")
                parent_id = comment.get("pk") or comment.get("id")
                if not include_replies or not parent_id or not (previews or expected):
                    continue
                remaining = None if max_items is None else max_items - len(delivered)
                if max_replies_per_comment is not None:
                    remaining = (max_replies_per_comment if remaining is None
                                 else min(remaining, max_replies_per_comment))
                if remaining == 0:
                    continue
                children = self.iter_replies(pk, parent_id, max_items=remaining,
                                             initial=previews, expected_items=expected,
                                             first_page=(reply_pages or {}).get(str(parent_id)))
                try:
                    for reply in children:
                        key = identity(reply)
                        if key in delivered:
                            continue
                        delivered.add(key)
                        yield {**reply, "_parent_comment_id": parent_id}
                        if max_items is not None and len(delivered) >= max_items:
                            report["stopReason"] = "results_limit"
                            return
                finally:
                    children.close()
            # The media count can include replies. Compare only after walking
            # both levels, and never imply full coverage from an ended cursor.
            reported = report.get("reportedCommentCount")
            if (include_replies and max_replies_per_comment is None
                    and isinstance(reported, int) and reported > len(delivered)):
                if "reported_count_gap" not in report["issues"]:
                    report["issues"].append("reported_count_gap")
                if report["stopReason"] == "source_exhausted":
                    report["stopReason"] = "reported_count_gap"
        finally:
            report["outputItems"] = len(delivered)
            parents.close()

    def iter_replies(
        self, media_id: str | int, comment_id: str | int, *,
        max_items: int | None = None, initial: list[dict[str, Any]] | None = None,
        expected_items: int | None = None,
        first_page: dict[str, Any] | None = None,
    ) -> Iterator[dict[str, Any]]:
        """Yield previews and all available head/tail reply pages, deduplicated."""
        from .comment_pages import walk
        pk = split_media_id(media_id)
        report = {"kind": "replies", "mediaId": str(pk),
                  "parentCommentId": str(comment_id), "expectedItems": expected_items,
                  "apiRequests": 0, "reusedFirstPage": first_page is not None}
        self.pagination_log.append(report)
        def fetch(**params):
            nonlocal first_page
            if not params and first_page is not None:
                body, first_page = first_page, None
                return body
            if self.pause and report.get("pages", 0) > 1:
                time.sleep(self.pause * random.uniform(0.7, 1.4))
            report["apiRequests"] += 1
            return self.ctx.api_get(ep.child_comments(pk, comment_id, **params),
                                    what=f"replies to comment {comment_id}")
        yield from walk(fetch, replies=True, report=report, max_pages=self.max_comment_pages,
                        max_items=max_items, initial=initial)

    # ------------------------------------------------------- tags & places --

    def hashtag(self, tag: str) -> dict[str, Any]:
        """Hashtag header record (needs authentication)."""
        body = self._read_page(
            ep.tag_web_info(tag), referer=ep.tag_page(tag), what=f"hashtag #{tag}"
        )
        return body.get("data") or body

    def iter_hashtag_media(
        self,
        tag: str,
        *,
        tab: str = "recent",
        max_items: int | None = None,
    ) -> Iterator[dict[str, Any]]:
        """Yield posts under a hashtag (needs authentication)."""
        cursor: str | None = None
        page = 0
        seen = 0
        while True:
            page += 1
            payload: dict[str, Any] = {"include_persistent": "0", "tab": tab}
            if cursor:
                payload.update({"max_id": cursor, "page": page})
            body = self._read_page(
                ep.tag_sections(tag), body=ep.form_body(payload),
                referer=ep.tag_page(tag), what=f"hashtag #{tag}",
            )
            for media in _iter_section_media(body):
                yield media
                seen += 1
                if max_items is not None and seen >= max_items:
                    return
            cursor = body.get("next_max_id")
            if not cursor or not body.get("more_available"):
                return

    def location(self, location_id: str | int) -> dict[str, Any]:
        """Place header record (needs authentication)."""
        body = self._read_page(
            ep.location_web_info(location_id),
            referer=ep.location_page(location_id),
            what=f"location {location_id}",
        )
        data = body.get("data") or body
        native = data.get("native_location_data") if isinstance(data, dict) else None
        if isinstance(native, dict):
            # 2026-09-22 shape: the header sits under
            # ``native_location_data.location_info`` (mobile spelling:
            # ``location_id``, ``location_address``, ``media_count``) and the
            # grids under ``ranked`` / ``recent`` as layout sections.
            info = dict(native.get("location_info") or {})
            info.setdefault("id", info.get("location_id"))
            info["ranked"], info["recent"] = native.get("ranked"), native.get("recent")
            return info
        return data

    def iter_location_media(
        self,
        location_id: str | int,
        *,
        tab: str = "recent",
        max_items: int | None = None,
    ) -> Iterator[dict[str, Any]]:
        """Yield posts tagged at a place (needs authentication)."""
        cursor: str | None = None
        page = 0
        seen = 0
        while True:
            page += 1
            payload: dict[str, Any] = {"tab": tab}
            if cursor:
                payload.update({"max_id": cursor, "page": page})
            body = self._read_page(
                ep.location_sections(location_id), body=ep.form_body(payload),
                referer=ep.location_page(location_id), what=f"location {location_id}",
            )
            for media in _iter_section_media(body):
                yield media
                seen += 1
                if max_items is not None and seen >= max_items:
                    return
            cursor = body.get("next_max_id")
            if not cursor or not body.get("more_available"):
                return

    # --------------------------------------------------------------- search --

    def search(self, query: str, *, count: int = 30,
               accounts_only: bool = False, context: str = "blended") -> dict[str, Any]:
        """Search results (needs authentication).

        Args:
            accounts_only: use the account-specific endpoint, which returns
                noticeably more profiles than the blended one -- 20 against 5
                for the same query -- at the cost of returning no hashtags or
                places.
            context: ``topsearch`` section. Measured 2026-09-22: ``blended``
                answers users only (``hashtags: []``, ``places: []``);
                ``hashtag`` returns the hashtag list.
        """
        url = (ep.account_search(query, count=count) if accounts_only
               else ep.topsearch(query, count=count, context=context))
        return self.ctx.api_get(
            url, referer=ep.search_page(query), what=f"search {query!r}",
        )

    def search_places(self, query: str) -> dict[str, Any]:
        """Place search (needs authentication); hits are under ``items``."""
        return self.ctx.api_get(
            ep.places_search(query), referer=ep.search_page(query),
            what=f"place search {query!r}",
        )

    # -------------------------------------------------------------- stories --

    def stories(self, user_id: str | int) -> list[dict[str, Any]]:
        """Active story reel for a profile (needs authentication)."""
        body = self.ctx.api_get(
            ep.reels_media([user_id]), what=f"stories of user {user_id}"
        )
        reels = body.get("reels") or body.get("reels_media") or {}
        if isinstance(reels, dict):
            reel = reels.get(str(user_id)) or next(iter(reels.values()), {})
        else:
            reel = reels[0] if reels else {}
        return list((reel or {}).get("items") or [])

    def highlights(self, user_id: str | int) -> list[dict[str, Any]]:
        """Highlight tray entries for a profile (needs authentication)."""
        body = self.ctx.api_get(
            ep.highlights_tray(user_id), what=f"highlights of user {user_id}"
        )
        return list(body.get("tray") or [])

    # -------------------------------------------------------------- helpers --

    def ensure_visible(self, profile: dict[str, Any]) -> None:
        """Reject proven private inaccessibility; an omitted viewer flag is unknown."""
        if profile.get("is_private") and profile.get("followed_by_viewer") is False:
            raise PrivateProfileError(
                f"@{profile.get('username')} is private and this session does "
                "not follow it"
            )


def _iter_section_media(body: dict[str, Any]) -> Iterator[dict[str, Any]]:
    """Flatten the nested `sections -> layout_content -> medias` shape."""
    for section in body.get("sections") or []:
        content = section.get("layout_content") or {}
        buckets: list[Any] = []
        buckets.extend(content.get("medias") or [])
        for group in content.get("fill_items") or []:
            buckets.append(group)
        for group in content.get("one_by_two_item") or []:
            buckets.append(group)
        for bucket in buckets:
            if not isinstance(bucket, dict):
                continue
            media = bucket.get("media")
            if isinstance(media, dict):
                yield media
            elif isinstance(bucket.get("clips"), dict):
                for entry in bucket["clips"].get("items") or []:
                    if isinstance(entry.get("media"), dict):
                        yield entry["media"]


