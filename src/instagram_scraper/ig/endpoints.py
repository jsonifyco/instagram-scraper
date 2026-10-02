"""Instagram web endpoints and the headers they expect.

Two families are used:

``/api/v1/...``
    The private "mobile" API exposed to the web client.  Requires the
    ``x-ig-app-id`` header.  Served to logged-out visitors only for
    :func:`web_profile_info` (:data:`ANONYMOUS_READ_PATHS`); everything else
    answers with a login wall unless a ``sessionid`` cookie is present.

``/graphql/query``
    The Relay endpoint the current web app uses.  Needs a ``doc_id`` plus the
    ``lsd`` token scraped from the page.

Anything blocked in both families falls back to page scraping (see
:mod:`instagram_scraper.ig.ai`).
"""

from __future__ import annotations

import json
import re
from urllib.parse import quote, urlencode, urlsplit

__all__ = [
    "BASE", "WEB_APP_ID", "json_headers", "graphql_headers", "web_retired",
    "anonymous_read",
    "web_profile_info", "user_info", "user_feed", "user_clips", "user_tagged_feed",
    "media_info", "media_comments", "child_comments",
    "media_shortcode_web_info",
    "tag_web_info", "tag_sections", "location_web_info", "location_sections",
    "topsearch", "account_search",
    "reels_media", "highlights_tray", "post_embed", "profile_page",
    "tag_page", "location_page", "search_page", "stories_page",
]

BASE = "https://www.instagram.com"

#: The public web client's app id.  Stable for years; overridable per session
#: because the bootstrap reads the live value out of the page when it can.
WEB_APP_ID = "936619743392459"

ASBD_ID = "129477"

#: REST reads the logged-in web client no longer makes, with how Instagram
#: answered a confirmed web session (never JSON). The web app reads the same
#: data through GraphQL on the page itself: ``PolarisProfilePageContentQuery``
#: for the profile, ``PolarisProfilePostsQuery`` for its grid,
#: ``PolarisProfileTaggedTabContentQuery`` for the Tagged tab (HAR of the
#: logged-in app, 2026-09-29: no request to any path below).
WEB_RETIRED_PATHS = (
    re.compile(r"/api/v1/feed/user/"),                 # 200, HTML app page (2026-09-24)
    re.compile(r"/api/v1/clips/user/"),                # 200, HTML app page (2026-09-24)
    re.compile(r"/api/v1/usertags/\d+/feed/"),         # 429, HTML "Page Not Found" (2026-09-29)
    re.compile(r"/api/v1/users/\d+/info/"),            # 429, empty body (2026-09-29)
    re.compile(r"/api/v1/users/web_profile_info/"),    # 429, HTML "Page Not Found" (2026-09-22, -29)
)


def web_retired(url: str) -> bool:
    """True for a REST read Instagram does not serve to a logged-in web
    session (see :data:`WEB_RETIRED_PATHS`). Accepts a URL or a path."""
    path = urlsplit(url).path if "://" in url else url
    return any(pattern.match(path) for pattern in WEB_RETIRED_PATHS)


#: REST reads Instagram serves to logged-out web visitors. A login wall on one
#: of them means the exit IP is walled off, not that the read needs an
#: account: one anonymous exit IP met the wall after 0-16 profiles while a
#: fresh VM read the same profiles (2026-09-29 and -30). Any other read that
#: answers a login wall anonymously needs cookies whatever the IP.
ANONYMOUS_READ_PATHS = (
    re.compile(r"/api/v1/users/web_profile_info/"),
)


def anonymous_read(url: str) -> bool:
    """True for a read a fresh exit IP can serve without cookies (see
    :data:`ANONYMOUS_READ_PATHS`). Accepts a URL or a path."""
    path = urlsplit(url).path if "://" in url else url
    return any(pattern.match(path) for pattern in ANONYMOUS_READ_PATHS)


def json_headers(
    *,
    app_id: str = WEB_APP_ID,
    referer: str | None = None,
    csrf: str | None = None,
    extra: dict[str, str] | None = None,
) -> dict[str, str]:
    """Headers for a ``/api/v1/`` XHR."""
    headers = {
        "x-ig-app-id": app_id,
        "x-asbd-id": ASBD_ID,
        "x-requested-with": "XMLHttpRequest",
        "accept": "*/*",
        "sec-fetch-site": "same-origin",
        "sec-fetch-mode": "cors",
        "sec-fetch-dest": "empty",
    }
    if referer:
        headers["referer"] = referer
    if csrf:
        headers["x-csrftoken"] = csrf
    if extra:
        headers.update(extra)
    return headers


def graphql_headers(
    *,
    lsd: str | None = None,
    csrf: str | None = None,
    app_id: str = WEB_APP_ID,
    referer: str | None = None,
    friendly_name: str | None = None,
) -> dict[str, str]:
    """Headers for a ``/graphql/query`` POST."""
    headers = json_headers(app_id=app_id, referer=referer, csrf=csrf)
    headers["content-type"] = "application/x-www-form-urlencoded"
    headers["x-fb-friendly-name"] = friendly_name or "PolarisAPI"
    if lsd:
        headers["x-fb-lsd"] = lsd
    return headers


def _q(params: dict[str, object]) -> str:
    clean = {k: v for k, v in params.items() if v is not None}
    return urlencode(clean, quote_via=quote)


# ---------------------------------------------------------------- profiles --

def web_profile_info(username: str) -> str:
    """Profile header + first page of the timeline.  Works logged out."""
    return f"{BASE}/api/v1/users/web_profile_info/?{_q({'username': username})}"


def user_info(user_id: str | int) -> str:
    """Profile header by numeric user id (what Apify's numeric `directUrls`
    entries name).  Answers the username, which the rest of the scraper
    keys on."""
    return f"{BASE}/api/v1/users/{user_id}/info/"


def user_feed(user_id: str | int, *, count: int = 12, max_id: str | None = None) -> str:
    """Paginated profile timeline (REST). Refused to logged-out web callers
    (401 ``require_login``) and not served to a confirmed web session
    (:data:`WEB_RETIRED_PATHS`); tried only outside one."""
    return f"{BASE}/api/v1/feed/user/{user_id}/?{_q({'count': count, 'max_id': max_id})}"


def user_clips(user_id: str | int, *, page_size: int = 12, max_id: str | None = None) -> str:
    """Reels tab of a profile (POST endpoint; body carries the cursor)."""
    return f"{BASE}/api/v1/clips/user/"


def user_tagged_feed(user_id: str | int, *, count: int = 12,
                     max_id: str | None = None) -> str:
    """Posts other accounts tagged this profile in -- the `mentions` source."""
    return f"{BASE}/api/v1/usertags/{user_id}/feed/?{_q({'count': count, 'max_id': max_id})}"


# ------------------------------------------------------------------- media --

def media_info(media_id: str | int) -> str:
    return f"{BASE}/api/v1/media/{media_id}/info/"


def media_shortcode_web_info(shortcode: str) -> str:
    return f"{BASE}/api/v1/media/shortcode/{shortcode}/web_info/"


def media_comments(
    media_id: str | int,
    *,
    min_id: str | None = None,
    max_id: str | None = None,
    sort_order: str | None = None,
    threading: bool = True,
) -> str:
    params: dict[str, object] = {
        "can_support_threading": "true" if threading else "false",
        "permalink_enabled": "false",
    }
    if min_id:
        params["min_id"] = min_id
    if max_id:
        params["max_id"] = max_id
    if sort_order:
        params["sort_order"] = sort_order
    return f"{BASE}/api/v1/media/{media_id}/comments/?{_q(params)}"


def child_comments(media_id: str | int, comment_id: str | int, *,
                   min_id: str | None = None, max_id: str | None = None,
                   is_chronological: bool | None = None,
                   paging_direction: str | None = None) -> str:
    """Replies to one comment. Head cursor goes to min_id, tail to max_id.

    Page sizes and available directions depend on the endpoint response;
    neither the first page size nor a repeated cursor is a universal ceiling.
    """
    params: dict[str, object] = {}
    if min_id:
        params["min_id"] = min_id
    if max_id:
        params["max_id"] = max_id
    # Instagram's direct-comment UI continues the head of a reply thread
    # with this exact contract. A bare min_id can replay the first rows and
    # was the source of false no_progress stops on large threads.
    if min_id is not None:
        params["is_chronological"] = "true" if is_chronological is not False else "false"
        params["paging_direction"] = paging_direction or "view_more"
    elif is_chronological is not None:
        params["is_chronological"] = "true" if is_chronological else "false"
    if paging_direction is not None and "paging_direction" not in params:
        params["paging_direction"] = paging_direction
    query = f"?{_q(params)}" if params else ""
    return f"{BASE}/api/v1/media/{media_id}/comments/{comment_id}/child_comments/{query}"


# --------------------------------------------------------- tags & places ---

def tag_web_info(tag: str) -> str:
    return f"{BASE}/api/v1/tags/web_info/?{_q({'tag_name': tag})}"


def tag_sections(tag: str) -> str:
    """POST endpoint; body carries ``tab`` and the pagination cursor."""
    return f"{BASE}/api/v1/tags/{quote(tag)}/sections/"


def location_web_info(location_id: str | int) -> str:
    return f"{BASE}/api/v1/locations/web_info/?{_q({'location_id': location_id, 'show_nearby': 'true'})}"


def location_sections(location_id: str | int) -> str:
    """POST endpoint; body carries ``tab`` and the pagination cursor."""
    return f"{BASE}/api/v1/locations/{location_id}/sections/"


# ------------------------------------------------------------------ search --

def topsearch(query: str, *, count: int = 30, context: str = "blended") -> str:
    """Blended search over accounts, hashtags and places.

    Note the path: ``/web/search/topsearch/`` answers 200 with a session while
    ``/api/v1/fbsearch/topsearch/`` -- the spelling most write-ups use --
    returns 404 even when authenticated.
    """
    return f"{BASE}/web/search/topsearch/?{_q({'context': context, 'query': query, 'count': count})}"


def places_search(query: str) -> str:
    """Places only. Measured 2026-09-22: the blended ``topsearch`` answers
    ``places: []`` even with ``context=place``, while this endpoint returns
    the venue list (30 for "Paris")."""
    return f"{BASE}/api/v1/fbsearch/places/?{_q({'query': query})}"


def account_search(query: str, *, count: int = 30) -> str:
    """Accounts only, and deeper than the blended search: 20 hits vs 5 for the
    same query.  Used when `searchType` is `user`."""
    return f"{BASE}/api/v1/fbsearch/account_serp/?{_q({'query': query, 'count': count})}"


# ----------------------------------------------------------------- stories --

def reels_media(user_ids: list[str | int]) -> str:
    params = [("reel_ids", str(uid)) for uid in user_ids]
    return f"{BASE}/api/v1/feed/reels_media/?{urlencode(params, quote_via=quote)}"


def highlights_tray(user_id: str | int) -> str:
    return f"{BASE}/api/v1/highlights/{user_id}/highlights_tray/"


# -------------------------------------------------------------- page URLs ---

def post_embed(shortcode: str) -> str:
    """Oembed-style render of a post.  Works logged out -- the anonymous
    fallback for post details."""
    return f"{BASE}/p/{shortcode}/embed/captioned/"


def profile_page(username: str) -> str:
    return f"{BASE}/{username}/"


def tag_page(tag: str) -> str:
    return f"{BASE}/explore/tags/{quote(tag)}/"


def location_page(location_id: str | int, slug: str = "") -> str:
    tail = f"{slug}/" if slug else ""
    return f"{BASE}/explore/locations/{location_id}/{tail}"


def search_page(query: str) -> str:
    return f"{BASE}/explore/search/keyword/?{_q({'q': query})}"


def stories_page(username: str) -> str:
    return f"{BASE}/stories/{username}/"


# ------------------------------------------------------------------ bodies --

def form_body(payload: dict[str, object]) -> str:
    """URL-encoded body for the POST-style private endpoints."""
    return _q(payload)


def graphql_body(doc_id: str, variables: dict[str, object], *,
                 lsd: str | None = None, friendly_name: str | None = None,
                 base: dict[str, object] | None = None) -> str:
    """Body for ``POST /graphql/query``."""
    payload: dict[str, object] = dict(base or {})
    payload.update({
        "doc_id": doc_id,
        "variables": json.dumps(variables, separators=(",", ":")),
        "server_timestamps": "true",
    })
    if lsd:
        payload["lsd"] = lsd
    if friendly_name:
        payload["fb_api_req_friendly_name"] = friendly_name
    return _q(payload)
