"""Anonymous post details via Instagram's embed renderer.

``/p/<code>/embed/captioned/`` is served without a login wall, which makes it
the fallback when ``/api/v1/media/<pk>/info/`` is gated.  It carries the owner,
the caption, the display image and (for videos) the source URL -- enough to
fill the core of a post record.  Engagement counters are usually absent from
the embed, so those come back as ``None`` and the caller decides whether to
escalate to the AI fallback.
"""

from __future__ import annotations

import json
import logging
import re
from typing import Any

from ..bro import commands as cmd
from ..errors import NotFoundError
from . import endpoints as ep
from .context import IgContext

log = logging.getLogger(__name__)

#: Runs inside the embed page and pulls every field the markup exposes.
_EMBED_JS = """
(() => {
  const text = (sel) => {
    const el = document.querySelector(sel);
    return el ? (el.textContent || '').trim() : null;
  };
  const attr = (sel, name) => {
    const el = document.querySelector(sel);
    return el ? el.getAttribute(name) : null;
  };
  const html = document.documentElement.outerHTML;
  const pick = (re) => { const m = html.match(re); return m ? m[1] : null; };

  const owner = text('.UsernameText') || text('.CaptionUsername')
             || attr('a.Username', 'href');
  let caption = text('.Caption');
  if (caption && owner && caption.indexOf(owner) === 0) {
    caption = caption.slice(owner.length).trim();
  }
  if (caption) {
    // The embed appends a trailing "<n> likes" / "view all comments" chrome.
    caption = caption.replace(/\\s*A post shared by[\\s\\S]*$/, '').trim();
  }

  const img = document.querySelector('img.EmbeddedMediaImage')
           || document.querySelector('.EmbeddedMediaImage img')
           || document.querySelector('img[srcset]');
  const video = document.querySelector('video');

  return JSON.stringify({
    ownerUsername: owner ? owner.replace(/^\\/+|\\/+$/g, '') : null,
    ownerFullName: text('.EmbedProfileName') || text('.FullName'),
    ownerProfilePic: attr('img.Avatar', 'src') || attr('.Avatar img', 'src'),
    caption: caption || null,
    displayUrl: img ? (img.getAttribute('src') || null) : null,
    videoUrl: video ? (video.getAttribute('src') || null) : null,
    isVideo: !!video || html.indexOf('EmbedIsVideo') !== -1,
    dimensionsWidth: img ? Number(img.getAttribute('width')) || null : null,
    dimensionsHeight: img ? Number(img.getAttribute('height')) || null : null,
    alt: img ? img.getAttribute('alt') : null,
    timestamp: attr('time', 'datetime'),
    likesText: text('.SocialProof') || text('.EmbedSocialProof'),
    permalink: attr('a.Button', 'href') || attr('.EmbedViewOnInstagram a', 'href'),
    mediaIdHint: pick(/"media_id":"(\\d+)"/) || pick(/"pk":"?(\\d{15,})"?/),
    notFound: html.indexOf('Sorry, this page') !== -1
           || html.indexOf('EmbedError') !== -1,
    title: document.title
  });
})()
"""

_LIKES_RE = re.compile(r"([\d.,]+)\s*(k|m|b)?\s*likes?", re.I)
_SUFFIX = {"k": 1_000, "m": 1_000_000, "b": 1_000_000_000}


def parse_count(text: str | None) -> int | None:
    """Turn ``"1,234 likes"`` / ``"12.3k likes"`` into an int."""
    if not text:
        return None
    match = _LIKES_RE.search(text)
    if not match:
        return None
    number, suffix = match.group(1), (match.group(2) or "").lower()
    try:
        value = float(number.replace(",", ""))
    except ValueError:
        return None
    return int(value * _SUFFIX.get(suffix, 1))


def fetch_embed(ctx: IgContext, shortcode: str) -> dict[str, Any]:
    """Load the embed page and return the fields it exposes.

    Raises:
        NotFoundError: the post is gone or was never public.
    """
    url = ep.post_embed(shortcode)
    log.debug("embed fallback for %s", shortcode)
    ctx.session.run([cmd.open_url(url), cmd.sleep(1.5)])
    ctx.current_page = url

    raw = ctx.session.js(_EMBED_JS, out_type="str")
    try:
        data = json.loads(raw) if isinstance(raw, str) else (raw or {})
    except json.JSONDecodeError:
        log.debug("embed JS returned non-JSON for %s: %.200s", shortcode, raw)
        data = {}

    if not data or data.get("notFound"):
        raise NotFoundError(f"post {shortcode} is unavailable via the embed view")

    data["likesCount"] = parse_count(data.get("likesText"))
    data["shortCode"] = shortcode
    data["url"] = ep.BASE + f"/p/{shortcode}/"
    return data


def looks_empty(data: dict[str, Any]) -> bool:
    """True when the embed gave us nothing worth keeping."""
    return not (data.get("caption") or data.get("displayUrl") or data.get("ownerUsername"))
