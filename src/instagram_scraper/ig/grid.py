"""Harvesting a profile grid by scrolling it.

Earlier measurements found the logged-out Relay preview limited to 12 posts;
the user feed also failed for that web context. The authenticated grid can
load additional tiles as it scrolls and remains a fallback when API paging is
unavailable. Those measurements are endpoint-specific, not a universal limit.

Two things make that harder than it looks:

* **The grid is virtualised.** Tile counts observed while scrolling @nasa went
  24, 36, 36, 48, 42, 45, 39, 48 ... -- React recycles DOM nodes, so the page
  holds only ~40-50 tiles at any moment and old ones disappear. Reading the DOM
  once at the end loses almost everything; the codes must be unioned after
  every scroll step.
* **Scroll height is the honest progress signal.** The tile count oscillates,
  so "did we advance?" is answered by ``scrollHeight`` growing and by new codes
  appearing, not by how many tiles are on screen.

Optionally retain complete media JSON that the browser already received.
Sparse tiles still require a media lookup by the caller.
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Iterator

from ..bro import commands as cmd
from .context import IgContext
from .network import NetworkCapture

log = logging.getLogger(__name__)

__all__ = ["GridHarvest", "collect_shortcodes", "iter_grid"]

#: Reads every post link currently in the DOM. Cheap enough to run after each
#: scroll, which is what makes the virtualised grid survivable.
_COLLECT_JS = r"""
(() => {
  const codes = [];
  const seen = {};
  document.querySelectorAll('a[href*="/p/"], a[href*="/reel/"], a[href*="/tv/"]')
    .forEach(a => {
      const m = (a.getAttribute('href') || '')
        .match(/\/(?:p|reel|tv)\/([A-Za-z0-9_-]+)/);
      if (m && !seen[m[1]]) { seen[m[1]] = 1; codes.push(m[1]); }
    });
  return JSON.stringify({
    codes,
    height: document.documentElement.scrollHeight,
    atEnd: (window.innerHeight + window.scrollY) >=
           (document.documentElement.scrollHeight - 200)
  });
})()
"""


@dataclass
class GridHarvest:
    """What one scroll-and-collect pass produced."""

    shortcodes: list[str] = field(default_factory=list)
    scrolls: int = 0
    seconds: float = 0.0
    #: True when the page stopped growing before we hit the requested count
    exhausted: bool = False
    media: dict[str, dict[str, Any]] = field(default_factory=dict)

    def __len__(self) -> int:
        return len(self.shortcodes)


def iter_grid(
    ctx: IgContext,
    url: str,
    *,
    want: int,
    max_scrolls: int = 60,
    pause: float = 2.0,
    stagnant_limit: int = 3,
    navigate: bool = True,
    capture_network: bool = False,
) -> Iterator[GridHarvest]:
    """Scroll a profile grid, unioning every shortcode that passes through.

    Args:
        ctx: browsing context, ideally authenticated; earlier anonymous probes
            showed only a short preview, so coverage depends on current access.
        url: the profile (or tagged/reels) page to harvest.
        want: stop once this many distinct shortcodes have been seen.
        max_scrolls: hard ceiling on scroll steps.
        pause: seconds to wait after each scroll for tiles to render.
        stagnant_limit: give up after this many consecutive steps that add no
            new codes *and* do not grow the page.
        navigate: set False when the browser is already on the page.

    Returns:
        A :class:`GridHarvest`; ``shortcodes`` preserves discovery order, which
        is newest-first for a profile grid.
    """
    if want <= 0:
        return
    if navigate:
        ctx.goto(url, wait=5.0)

    started = time.monotonic()
    ordered: dict[str, None] = {}
    last_height = 0
    stagnant = 0
    viewport = 0
    scrolls_done = 0
    network = NetworkCapture(stop_on_rejection=bool(getattr(ctx, "authenticated", False)),
                             rejection_since=getattr(ctx, "authenticated_since", None)) if capture_network else None
    pending = []

    while len(ordered) < want and viewport <= max_scrolls:
        # Sample viewport zero before React can recycle the initial tiles.
        if viewport:
            ctx.session.run([cmd.scroll_to_viewport(viewport), cmd.sleep(pause)])
            scrolls_done += 1

        raw = ctx.session.js(_COLLECT_JS, out_type="str")
        try:
            data = json.loads(raw) if isinstance(raw, str) else (raw or {})
        except json.JSONDecodeError:
            log.debug("grid JS returned non-JSON: %.160s", raw)
            data = {}
        if not isinstance(data, dict):
            data = {}

        before = len(ordered)
        for code in data.get("codes") or []:
            if isinstance(code, str) and code not in ordered:
                ordered[code] = None
                if len(ordered) <= want:
                    pending.append(code)
        height = int(data.get("height") or 0)

        gained = len(ordered) - before
        grew = height > last_height
        log.debug("grid viewport %d: +%d codes (total %d), height %d -> %d",
                  viewport, gained, len(ordered), last_height, height)

        # The tile count oscillates because the grid recycles nodes, so treat
        # "no new codes AND no taller page" as the only real stall signal.
        stagnant = 0 if (gained or grew) else stagnant + 1
        checkpoint = (viewport % 5 == 0 or len(ordered) >= want
                      or stagnant >= stagnant_limit or viewport == max_scrolls)
        if network and checkpoint:
            network.poll(ctx.session, codes=set(pending))
        if checkpoint or not network:
            yield GridHarvest(shortcodes=list(pending), scrolls=scrolls_done,
                              seconds=round(time.monotonic() - started, 1),
                              exhausted=stagnant >= stagnant_limit,
                              media={c: network.media[c] for c in pending if network and c in network.media})
            pending.clear()
            if network:
                network.media.clear()
        if stagnant >= stagnant_limit:
            log.info("grid stopped growing after %d scroll(s) with %d code(s)",
                     viewport, len(ordered))
            break

        last_height = max(last_height, height)
        viewport += 1


def collect_shortcodes(ctx, url, **kwargs):
    """The whole harvest at once (the posts and details scrapers); complete
    mode consumes :func:`iter_grid` batch by batch."""
    harvest = GridHarvest()
    for batch in iter_grid(ctx, url, **kwargs):
        harvest.shortcodes.extend(batch.shortcodes)
        harvest.media.update(batch.media)
        harvest.scrolls, harvest.seconds, harvest.exhausted = batch.scrolls, batch.seconds, batch.exhausted
    return harvest
