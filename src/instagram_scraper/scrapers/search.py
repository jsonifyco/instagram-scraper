"""Discovery mode: turn a `search` query into concrete targets.

`searchType` decides what a query resolves to (hashtags, profiles or places).
The resolved entries are both emitted as records *and* handed back to the
runner as targets, so `search` + `resultsType: posts` behaves the same way it
does upstream: find the accounts, then scrape their posts.
"""

from __future__ import annotations

import logging
from typing import Any, Iterator

from ..errors import InstagramError, LoginRequiredError
from ..input_model import SearchType
from ..mappers.profile import map_search_hit
from ..urls import Target, TargetType
from .base import BaseScraper

log = logging.getLogger(__name__)

_SECTION_FOR = {
    SearchType.HASHTAG: ("hashtags", "hashtag"),
    SearchType.USER: ("users", "user"),
    SearchType.PLACE: ("items", "place"),
}


class SearchScraper(BaseScraper):
    """Resolves search queries into records and follow-up targets."""

    name = "search"

    def run(self, target: Target) -> Iterator[dict[str, Any]]:
        yield from self.limited(self._search(target.key, target.input_url),
                                self.config.search_limit)

    def should_fall_back(self, error: Exception) -> bool:
        """Search always falls back, and also falls back on 404.

        ``/api/v1/fbsearch/topsearch/`` answers 404 (not 401) for a logged-out
        caller: the endpoint is absent rather than the query being unknown, so
        unlike elsewhere a missing resource here means "resolve it another
        way".  The fallback is direct resolution rather than a vision pass, so
        it costs nothing and is not gated on `aiFallback`.
        """
        return True

    def queries(self) -> list[str]:
        """Split the comma-separated `search` input into individual queries."""
        raw = self.config.search
        return [part.strip() for part in raw.split(",") if part.strip()]

    def resolve(self, query: str) -> list[dict[str, Any]]:
        """All hits for one query, capped at `searchLimit`."""
        return list(self._search(query, query))[: self.config.search_limit]

    # --------------------------------------------------------------- lookup --

    def _search(self, query: str, input_url: str) -> Iterator[dict[str, Any]]:
        section, kind = _SECTION_FOR[self.config.search_type]
        try:
            if self.config.search_type is SearchType.PLACE:
                # the blended search stopped listing places (2026-09-22)
                body = self.api.search_places(query)
            else:
                body = self.api.search(
                    query, count=max(self.config.search_limit, 30),
                    # The account endpoint returns four times as many profiles as
                    # the blended one, so prefer it when profiles are all we want.
                    accounts_only=self.config.search_type is SearchType.USER,
                    # ... and the blended one stopped listing hashtags (2026-09-22)
                    context="hashtag" if self.config.search_type is SearchType.HASHTAG else "blended",
                )
        except InstagramError as exc:
            if not self.should_fall_back(exc):
                raise
            self.note_fallback(f"topsearch:{type(exc).__name__}")
            yield from self._ai_search(query, input_url)
            return

        entries = body.get(section) or []
        if not entries:
            log.info("[search] no %s results for %r", kind, query)
        for entry in entries:
            record = map_search_hit(entry, kind)
            record["inputUrl"] = input_url
            record["searchQuery"] = query
            record["dataSource"] = "api"
            yield record

    def _ai_search(self, query: str, input_url: str) -> Iterator[dict[str, Any]]:
        """Anonymous degradation: resolve the query directly instead of ranking.

        Instagram's search surface redirects a logged-out visitor to
        ``/accounts/login/``, so neither the API nor the rendered page can be
        read and the vision model has nothing to work with.  What still works
        without a session is treating the query as the thing itself: a
        hashtag query names a tag page, and a profile query names a handle we
        can verify through the (ungated) profile endpoint.  Ranked discovery
        and place lookup genuinely need `sessionCookies`.
        """
        search_type = self.config.search_type

        if search_type is SearchType.PLACE:
            raise LoginRequiredError(
                f"places matching {query!r}. A place query resolves to a numeric "
                "location id, which only Instagram's search API can supply -- "
                "pass `sessionCookies`, or give the place URL in `directUrls`"
            )

        if search_type is SearchType.HASHTAG:
            name = query.strip().lstrip("#").lower()
            log.info("[search] resolving %r directly to #%s (no session; "
                     "ranked discovery unavailable)", query, name)
            yield {
                "type": "hashtag",
                "name": name,
                "url": f"https://www.instagram.com/explore/tags/{name}/",
                "inputUrl": input_url,
                "searchQuery": query,
                "dataSource": "direct",
            }
            return

        handle = query.strip().lstrip("@").strip("/")
        try:
            profile = self.scrape.profile(handle)
        except InstagramError as exc:
            raise LoginRequiredError(
                f"profiles matching {query!r}. Ranked profile search needs a "
                f"session; resolving {query!r} as the literal handle @{handle} "
                f"also failed ({type(exc).__name__}). Pass `sessionCookies`, or "
                "list the profile URLs in `directUrls`"
            ) from None

        log.info("[search] resolving %r directly to @%s (no session; ranked "
                 "discovery unavailable)", query, handle)
        yield {
            "type": "user",
            "id": str(profile.get("id") or ""),
            "username": profile.get("username") or handle,
            "fullName": profile.get("full_name"),
            "url": f"https://www.instagram.com/{profile.get('username') or handle}/",
            "verified": bool(profile.get("is_verified")),
            "private": bool(profile.get("is_private")),
            "profilePicUrl": profile.get("profile_pic_url"),
            "inputUrl": input_url,
            "searchQuery": query,
            "dataSource": "direct",
        }


#: what our records name in ``searchSource`` (Apify names its own discovery
#: sources there, e.g. ``facebook-ads``); ours is the Instagram endpoint
SEARCH_SOURCE = "instagram-topsearch"


def hits_to_targets(hits: list[dict[str, Any]], search_type: SearchType) -> list[Target]:
    """Convert search records into targets the other scrapers can consume.

    Each target remembers the query it came from (``extra.search_term``) so
    the records scraped for it can carry ``searchTerm`` / ``searchSource``
    the way Apify's do.
    """
    targets: list[Target] = []
    for hit in hits:
        extra = {"search_term": str(hit.get("searchQuery") or ""), "search_source": SEARCH_SOURCE}
        if search_type is SearchType.USER:
            username = hit.get("username") or hit.get("name")
            if username:
                if hit.get("id"):
                    # the numeric id lets an authenticated run read the
                    # profile by id and skip ``web_profile_info``
                    extra["user_id"] = str(hit["id"])
                targets.append(Target(
                    TargetType.PROFILE, str(username), hit.get("inputUrl") or str(username),
                    f"https://www.instagram.com/{username}/", extra,
                ))
        elif search_type is SearchType.HASHTAG:
            name = hit.get("name")
            if name:
                targets.append(Target(
                    TargetType.HASHTAG, str(name).lstrip("#"),
                    hit.get("inputUrl") or str(name),
                    f"https://www.instagram.com/explore/tags/{str(name).lstrip('#')}/", extra,
                ))
        else:
            place_id = hit.get("id")
            if place_id:
                targets.append(Target(
                    TargetType.PLACE, str(place_id), hit.get("inputUrl") or str(place_id),
                    f"https://www.instagram.com/explore/locations/{place_id}/", extra,
                ))
    return targets
