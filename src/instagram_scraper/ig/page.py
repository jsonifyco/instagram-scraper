"""Header data straight out of a rendered hashtag or place page.

Instagram server-renders the name, the post count and the category into the
markup of `/explore/tags/<name>/` and `/explore/locations/<id>/` even for a
logged-out visitor:

    <title>New York, New York on Instagram - Photos and Videos</title>
    <h1>New York, New York</h1>
    ... City - 83.4M posts ...

    <title>Space - 124M reels on Instagram</title>
    <h1>Space</h1>

Reading that costs one `run_js` call and no AI tokens, and it is more reliable
than asking a vision model to find the same two values -- during verification
the model returned a null name for a place whose `<h1>` said "New York, New
York".  So the details scrapers try this first and keep the AI pass as backup.
"""

from __future__ import annotations

import json
import logging
import re
import time
from typing import Any

from ..errors import LoginRequiredError
from . import endpoints as ep
from .context import IgContext

log = logging.getLogger(__name__)

__all__ = ["PageHeader", "read_header", "parse_amount", "read_user_id",
           "read_profile", "read_app_profile", "parse_og_description"]

#: Instagram server-renders a profile's counts into og:description:
#:   "104M Followers, 91 Following, 4,909 Posts - See Instagram photos and
#:    videos from NASA (@nasa)"
#: That one string carries followers, following, posts, full name and handle,
#: which is everything `details` needs when `web_profile_info` is throttled --
#: and it costs one `run_js` instead of a vision pass that returned nulls.
_PROFILE_JS = r"""
(() => {
  const meta = (sel) => {
    const el = document.querySelector(sel);
    return el ? el.getAttribute('content') : null;
  };
  const html = document.documentElement.outerHTML;
  const pick = (re) => { const m = html.match(re); return m ? m[1] : null; };
  return JSON.stringify({
    url: location.href,
    login: location.href.indexOf('/accounts/login') !== -1
        || !!document.querySelector('input[name="username"]'),
    description: meta('meta[property="og:description"]')
              || meta('meta[name="description"]'),
    title: document.title,
    image: meta('meta[property="og:image"]'),
    verified: html.indexOf('"is_verified":true') !== -1,
    private: html.indexOf('"is_private":true') !== -1,
    externalUrl: pick(/"external_url":"([^"]+)"/),
    bio: pick(/"biography":"((?:[^"\\]|\\.)*)"/),
    userId: pick(/"profilePage_(\d{4,})"/) || pick(/"user_id":"(\d{4,})"/)
  });
})()
"""

#: "104M Followers, 91 Following, 4,909 Posts - ... from NASA (@nasa)"
_OG_COUNTS_RE = re.compile(
    r"([\d][\d.,]*\s*[KMB]?)\s*Followers?,\s*"
    r"([\d][\d.,]*\s*[KMB]?)\s*Following,\s*"
    r"([\d][\d.,]*\s*[KMB]?)\s*Posts?",
    re.I,
)
_OG_NAME_RE = re.compile(r"from\s+(.+?)\s*\(@([A-Za-z0-9._]+)\)")

#: A rendered profile page names its owner's numeric id in several places.
#: Reading it here avoids `web_profile_info`, which is the first endpoint
#: Instagram throttles and is otherwise a single point of failure for every
#: user-id-keyed call (stories, tagged feed, highlights).
_USER_ID_JS = r"""
(() => {
  const html = document.documentElement.outerHTML;
  const patterns = [
    /"profilePage_(\d{4,})"/,
    /"user_id"\s*:\s*"(\d{4,})"/,
    /"owner"\s*:\s*\{\s*"id"\s*:\s*"(\d{4,})"/,
    /"props"\s*:\s*\{[^}]{0,200}"id"\s*:\s*"(\d{4,})"/,
    /instapp:owner_user_id"\s+content="(\d{4,})"/,
    /"X-IG-Target-User-Id"\s*:\s*"(\d{4,})"/
  ];
  const counts = {};
  for (const re of patterns) {
    const m = html.match(re);
    if (m) counts[m[1]] = (counts[m[1]] || 0) + 1;
  }
  // The viewer's own id also appears on the page; prefer whichever id the
  // markup mentions most, and report the viewer so the caller can exclude it.
  const viewer = (document.cookie.match(/ds_user_id=(\d+)/) || [])[1] || null;
  let best = null, bestCount = 0;
  for (const id in counts) {
    if (id !== viewer && counts[id] > bestCount) { best = id; bestCount = counts[id]; }
  }
  if (!best) { for (const id in counts) { best = id; break; } }
  return JSON.stringify({userId: best, viewer, candidates: counts});
})()
"""

_HEADER_JS = """
(() => {
  const meta = (sel) => {
    const el = document.querySelector(sel);
    return el ? el.getAttribute('content') : null;
  };
  const text = (sel) => {
    const el = document.querySelector(sel);
    return el ? (el.textContent || '').trim() : null;
  };
  return JSON.stringify({
    url: location.href,
    login: location.href.indexOf('/accounts/login') !== -1,
    title: document.title || null,
    h1: text('h1'),
    ogTitle: meta('meta[property="og:title"]'),
    ogImage: meta('meta[property="og:image"]'),
    description: meta('meta[name="description"]')
              || meta('meta[property="og:description"]'),
    bodyHead: (document.body.innerText || '').slice(0, 900)
  });
})()
"""

#: "83.4M posts", "124M reels", "1,234 posts"
_COUNT_RE = re.compile(r"([\d][\d.,]*)\s*([KMB])?\s*(?:posts?|reels?)", re.I)
_MULTIPLIER = {"k": 1_000, "m": 1_000_000, "b": 1_000_000_000}

#: strip the boilerplate Instagram appends to page titles
_TITLE_TAIL = re.compile(
    r"\s*(?:on Instagram.*|[•|]\s*\d[\d.,]*\s*[KMB]?\s*(?:posts?|reels?).*)$",
    re.I,
)


class PageHeader:
    """What a rendered tag/place page says about itself."""

    __slots__ = ("name", "posts_count", "category", "description",
                 "image_url", "raw")

    def __init__(
        self,
        *,
        name: str | None = None,
        posts_count: int | None = None,
        category: str | None = None,
        description: str | None = None,
        image_url: str | None = None,
        raw: dict[str, Any] | None = None,
    ) -> None:
        self.name = name
        self.posts_count = posts_count
        self.category = category
        self.description = description
        self.image_url = image_url
        self.raw = raw or {}

    @property
    def usable(self) -> bool:
        """True when the page told us at least the name."""
        return bool(self.name)

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "postsCount": self.posts_count,
            "category": self.category,
            "description": self.description,
            "profilePicUrl": self.image_url,
            "dataSource": "page",
        }

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return f"PageHeader(name={self.name!r}, posts_count={self.posts_count!r})"


def parse_amount(text: str | None) -> int | None:
    """Turn ``"83.4M posts"`` / ``"1,234 posts"`` into an int."""
    if not text:
        return None
    match = _COUNT_RE.search(text)
    if not match:
        return None
    number, suffix = match.group(1), (match.group(2) or "").lower()
    multiplier = _MULTIPLIER.get(suffix, 1)
    number = number.replace(",", "")
    # A dot is a decimal point only when a magnitude suffix follows.
    if multiplier == 1:
        number = number.replace(".", "")
    try:
        return int(float(number) * multiplier)
    except ValueError:
        return None


def read_header(ctx: IgContext, url: str, *, navigate: bool = True) -> PageHeader:
    """Read the header of a hashtag or place page.

    Args:
        ctx: browsing context.
        url: the page to read.
        navigate: set False when the browser is already parked on the page.

    Raises:
        LoginRequiredError: Instagram bounced the visitor to the login screen.
    """
    if navigate:
        ctx.goto(url, wait=4.0)

    raw = ctx.session.js(_HEADER_JS, out_type="str")
    try:
        data = json.loads(raw) if isinstance(raw, str) else (raw or {})
    except json.JSONDecodeError:
        log.debug("header JS returned non-JSON for %s: %.200s", url, raw)
        return PageHeader()

    if data.get("login"):
        raise LoginRequiredError(f"the page at {url}")

    title = data.get("title") or data.get("ogTitle") or ""
    name = data.get("h1") or _TITLE_TAIL.sub("", title).strip() or None
    if name:
        name = name.lstrip("#").strip() or None

    body = data.get("bodyHead") or ""
    count = parse_amount(title) or parse_amount(data.get("ogTitle")) or parse_amount(body)

    header = PageHeader(
        name=name,
        posts_count=count,
        category=_category_from_body(body, name),
        description=data.get("description"),
        image_url=data.get("ogImage"),
        raw=data,
    )
    log.debug("page header for %s: %r", url, header)
    return header


def read_user_id(ctx: IgContext, username: str, *, navigate: bool = True) -> str | None:
    """Resolve a username to its numeric id from the rendered profile page.

    The username API can reject reads even when a public profile page renders.
    Stories and the tagged feed need a numeric ID; inspect that page for one
    before giving up. Some pages expose no usable ID.

    Returns None when the page does not name an id (a login wall, or a profile
    that did not render).
    """
    if navigate:
        ctx.goto(f"{ep.BASE}/{username}/", wait=4.0)

    raw = ctx.session.js(_USER_ID_JS, out_type="str")
    try:
        data = json.loads(raw) if isinstance(raw, str) else (raw or {})
    except json.JSONDecodeError:
        log.debug("user-id JS returned non-JSON for @%s: %.160s", username, raw)
        return None

    user_id = data.get("userId")
    if not user_id:
        log.debug("no user id on the page for @%s (candidates=%s)",
                  username, data.get("candidates"))
        return None
    if user_id == data.get("viewer"):
        # Only the signed-in viewer's own id was found, which means the page
        # did not actually render the target profile.
        log.debug("page for @%s only named the viewer's own id", username)
        return None

    log.info("resolved @%s to user id %s from the page", username, user_id)
    return str(user_id)


def parse_og_description(text: str | None) -> dict[str, Any]:
    """Pull counts, full name and handle out of a profile's og:description."""
    out: dict[str, Any] = {}
    if not text:
        return out
    counts = _OG_COUNTS_RE.search(text)
    if counts:
        out["followersCount"] = parse_amount(counts.group(1) + " posts")
        out["followsCount"] = parse_amount(counts.group(2) + " posts")
        out["postsCount"] = parse_amount(counts.group(3) + " posts")
    name = _OG_NAME_RE.search(text)
    if name:
        out["fullName"] = name.group(1).strip() or None
        out["username"] = name.group(2)
    return out


def read_profile(ctx: IgContext, username: str, *,
                 navigate: bool = True) -> dict[str, Any]:
    """Read a profile's header straight off its rendered page.

    `web_profile_info` is the endpoint Instagram throttles first, and when it
    goes the `details` scraper had only the vision fallback left -- which came
    back with a null username for a page that plainly showed one. The page's
    own og:description carries the counts, the display name and the handle,
    for one `run_js` and no AI tokens.

    Returns a dict shaped like the `web_profile_info` user object, so it feeds
    the existing mapper unchanged. Empty when the page did not render.
    """
    if navigate:
        ctx.goto(f"{ep.BASE}/{username}/", wait=4.0)

    raw = ctx.session.js(_PROFILE_JS, out_type="str")
    try:
        data = json.loads(raw) if isinstance(raw, str) else (raw or {})
    except json.JSONDecodeError:
        log.debug("profile JS returned non-JSON for @%s: %.160s", username, raw)
        return {}

    if data.get("login"):
        raise LoginRequiredError(f"the profile page of @{username}")

    parsed = parse_og_description(data.get("description"))
    handle = parsed.get("username") or username
    if not parsed and not data.get("userId"):
        log.debug("profile page for @%s carried no header", username)
        return {}

    record: dict[str, Any] = {
        "username": handle,
        "full_name": parsed.get("fullName"),
        "biography": _unescape(data.get("bio")),
        "external_url": data.get("externalUrl"),
        "profile_pic_url": data.get("image"),
        "is_verified": bool(data.get("verified")),
        "is_private": bool(data.get("private")),
        "edge_followed_by": {"count": parsed.get("followersCount")},
        "edge_follow": {"count": parsed.get("followsCount")},
        "edge_owner_to_timeline_media": {"count": parsed.get("postsCount")},
        "dataSource": "page",
    }
    if data.get("userId"):
        record["id"] = str(data["userId"])
    log.info("read @%s off the page: %s followers, %s posts",
             handle, parsed.get("followersCount"), parsed.get("postsCount"))
    return record


def read_app_profile(ctx: IgContext, username: str, *, navigate: bool = True,
                     polls: int = 2, settle: float = 3.0) -> dict[str, Any]:
    """A profile as the logged-in web app itself loads it.

    A confirmed web session is not served ``users/{id}/info`` or
    ``web_profile_info`` (429 without JSON, 2026-09-29); the profile page
    receives the same header through ``PolarisProfilePageContentQuery`` and
    its first grid page through ``PolarisProfilePostsQuery`` (HAR of the
    logged-in app). Both are read from the page's own responses -- no
    request is replayed -- and shaped like ``web_profile_info``: the header
    fields plus ``edge_owner_to_timeline_media`` with the grid's posts, so
    ``latestPosts`` and the posts timeline work unchanged.

    Returns an empty dict when the page carried no profile object.
    """
    from .network import APP_RESOURCE_TYPES, NetworkCapture

    if navigate:
        ctx.goto(ep.profile_page(username), wait=5.0)
    capture = NetworkCapture(stop_on_rejection=bool(getattr(ctx, "authenticated", False)),
                             rejection_since=getattr(ctx, "authenticated_since", None))
    key = username.lower()
    for attempt in range(max(1, polls)):
        if attempt:
            # The header query can land a moment after the page settles.
            time.sleep(settle)
        capture.poll(ctx.session, resource_types=APP_RESOURCE_TYPES)
        if key in capture.profiles or capture.disabled:
            break
    raw = capture.profiles.get(key)
    if not raw:
        log.info("the profile page of @%s carried no profile object", username)
        return {}
    record = dict(raw)
    record["id"] = str(raw.get("id") or raw.get("pk"))
    friendship = raw.get("friendship_status")
    if "followed_by_viewer" not in record and isinstance(friendship, dict) and "following" in friendship:
        # what `ensure_visible` checks on a private profile
        record["followed_by_viewer"] = bool(friendship["following"])
    if record.get("fbid") is None and raw.get("fbid_v2"):
        record["fbid"] = str(raw["fbid_v2"])
    timeline = list((capture.timelines.get(key) or {}).values())
    if timeline and not (record.get("edge_owner_to_timeline_media") or {}).get("edges"):
        record["edge_owner_to_timeline_media"] = {
            "count": raw.get("media_count"),
            "edges": [{"node": node} for node in timeline],
        }
    record["dataSource"] = "network"
    log.info("read @%s from the page's own profile response: %s followers, %s posts, "
             "%d grid post(s)", record.get("username"), raw.get("follower_count"),
             raw.get("media_count"), len(timeline))
    return record


def _unescape(text: str | None) -> str | None:
    """Undo the JSON escaping of a value lifted out of raw page HTML."""
    if not text:
        return None
    try:
        return json.loads(f'"{text}"')
    except json.JSONDecodeError:
        return text


def _category_from_body(body: str, name: str | None) -> str | None:
    """The short label a place page prints under its name (e.g. "City")."""
    if not body or not name:
        return None
    lines = [line.strip() for line in body.splitlines() if line.strip()]
    try:
        index = lines.index(name)
    except ValueError:
        return None
    for candidate in lines[index + 1: index + 3]:
        if candidate in ("•", "-"):
            continue
        if _COUNT_RE.search(candidate):
            return None
        if 2 <= len(candidate) <= 40:
            return candidate
    return None
