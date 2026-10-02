"""Builders for the getbro command payloads this scraper uses.

Each builder delegates to the SDK's typed builder in ``bro.commands`` -- the
command names, parameter spellings and validation are the SDK's -- and
returns the payload as a plain dict (``{"command": ..., "params": {...}}``),
which is what the engine, the operation journal and the tests work with.
Our own defaults (wheel position for ``scroll``, ``out_type`` for
``run_js``, a list for ``press``) live in the wrappers, and a parameter left
``None`` is omitted from the wire exactly as before. Batches read like a
script:

    session.run([
        commands.open_url(url),
        commands.sleep(2),
        commands.run_js("document.title", out_type="str"),
    ])
"""

from __future__ import annotations

from typing import Any, Iterable

from bro import commands as _sdk

__all__ = [
    "open_url", "get_url", "refresh", "sleep", "run_js", "fetch_json", "locate",
    "get_html", "get_screenshot", "scroll", "scroll_down", "scroll_to_viewport",
    "inject_cookies", "inject_storage", "dump_cookies", "dump_local_storage",
    "dump_console_logs", "dump_har_logs", "HAR_RESOURCE_TYPES", "extract", "act", "press", "click_at", "click",
]


def _dump(payload: Any) -> dict[str, Any]:
    """The SDK's ``CommandPayload`` as the dict shape the engine journals."""
    data = payload.model_dump(exclude_none=True)
    params = {k: v for k, v in (data.get("params") or {}).items() if v is not None}
    return {"command": data["command"], "params": params} if params else {"command": data["command"]}


# ------------------------------------------------------- navigation & state --

def open_url(url: str) -> dict[str, Any]:
    return _dump(_sdk.open_url(url))


def get_url() -> dict[str, Any]:
    return _dump(_sdk.get_url())


def locate(*, strategy: str, value: str, state: str = "attached",
           timeout_ms: int = 15000) -> dict[str, Any]:
    """Wait for a DOM condition with the SDK's locator command."""
    return _dump(_sdk.locate(strategy=strategy, value=value,
                             state=state, timeout_ms=timeout_ms))


def refresh() -> dict[str, Any]:
    return _dump(_sdk.refresh())


def sleep(seconds: float, fluctuation: float | None = None) -> dict[str, Any]:
    return _dump(_sdk.sleep(float(seconds), fluctuation=fluctuation))


def get_html() -> dict[str, Any]:
    return _dump(_sdk.get_html())


def get_screenshot(**kwargs: Any) -> dict[str, Any]:
    return _dump(_sdk.get_screenshot(**kwargs))


def run_js(js_code: str, out_type: str | None = None, timeout: float | None = None) -> dict[str, Any]:
    """``timeout``: seconds before getbro aborts the script (SDK 0.1.8;
    0.1-180, getbro's default 30 when omitted)."""
    return _dump(_sdk.run_js(js_code, out_type=out_type, timeout=timeout))


def fetch_json(
    url: str,
    *,
    method: str = "GET",
    headers: dict[str, str] | None = None,
    body: str | None = None,
    timeout: float | None = None,
) -> dict[str, Any]:
    """In-page fetch that inherits the session's cookies, proxy and headers.
    ``timeout``: seconds before getbro aborts the request (SDK 0.1.8; 0.1-180,
    getbro's default 30 when omitted)."""
    return _dump(_sdk.fetch_json(url, method=method, headers=headers or {}, body=body,
                                 timeout=timeout))


# ------------------------------------------------------------------ input ---

def scroll(
    clicks: int = -5,
    *,
    x: float = 640,
    y: float = 500,
    axis: str = "y",
    scope: str | None = None,
) -> dict[str, Any]:
    """Mouse-wheel scroll.

    getbro counts wheel clicks with the sign pointing the way the *page*
    moves: negative scrolls down, positive scrolls up.  The cursor is parked
    at ``(x, y)`` first, defaulting to the middle of the 1280x1024 window so
    the wheel lands on the page body rather than a sidebar.
    """
    return _dump(_sdk.scroll(clicks, x, y, scope=scope, axis=axis))


def scroll_down(clicks: int = 5, **kwargs: Any) -> dict[str, Any]:
    """Readable wrapper: scroll `clicks` wheel notches toward the page bottom."""
    return scroll(-abs(clicks), **kwargs)


def scroll_to_viewport(viewport_idx: int) -> dict[str, Any]:
    """Jump straight to a 0-based viewport chunk -- cheaper than many scrolls."""
    return _dump(_sdk.scroll_to_viewport(viewport_idx))


def press(keys: list[str] | str) -> dict[str, Any]:
    return _dump(_sdk.press([keys] if isinstance(keys, str) else list(keys)))


def click_at(
    x: float,
    y: float,
    *,
    mouse_button: str | None = None,
    clicks: int | None = None,
    scope: str | None = None,
) -> dict[str, Any]:
    return _dump(_sdk.click_at(x, y, mouse_button=mouse_button, clicks=clicks, scope=scope))


# ------------------------------------------------------------------ state ---

def inject_cookies(cookies: list[dict[str, Any]]) -> dict[str, Any]:
    return _dump(_sdk.inject_cookies(cookies))


def inject_storage(items: list[dict[str, Any]], storage_type: str = "local") -> dict[str, Any]:
    """Seed localStorage/sessionStorage before navigation (CDP new-document script)."""
    return _dump(_sdk.inject_storage(items, storage_type=storage_type))


def dump_cookies() -> dict[str, Any]:
    return _dump(_sdk.dump_cookies())

def click(
    *,
    mouse_button: str | None = None,
    clicks: int | None = None,
    element_id: str | None = None,
    locator: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return _dump(_sdk.click(mouse_button=mouse_button, clicks=clicks, element_id=element_id, locator=locator))




def dump_local_storage() -> dict[str, Any]:
    return _dump(_sdk.dump_local_storage())


def dump_console_logs() -> dict[str, Any]:
    return _dump(_sdk.dump_console_logs())


#: every resource type getbro's archive can be filtered by
HAR_RESOURCE_TYPES = (
    "document", "stylesheet", "script", "image", "media", "font", "xhr",
    "fetch", "websocket", "eventsource", "manifest", "texttrack", "other",
)


def dump_har_logs(
    since: float | None = None,
    resource_types: Iterable[str] | None = None,
) -> dict[str, Any]:
    """Network archive for the session: every proxied request with headers,
    bodies and responses.  Used to learn what the page asks for on its own.

    ``since`` is a Unix timestamp in seconds; getbro keeps the entries whose
    request *started* at or after it (inclusive).  ``resource_types`` keeps
    only the listed types (see :data:`HAR_RESOURCE_TYPES`).  Both filter the
    export, not the archive itself.
    """
    types = [str(t) for t in resource_types] if resource_types is not None else None
    return _dump(_sdk.dump_har_logs(since=float(since) if since is not None else None,
                                    resource_types=types or None))


# --------------------------------------------------------------- autopilot --

def extract(
    data_instruction: str,
    json_schema: Any = None,
    *,
    model_size: str = "small",
    vision: bool | None = None,
    feed_urls: bool | None = None,
    viewport_min: int | None = None,
    viewport_max: int | None = None,
    paginate: bool | None = None,
    pages_to_paginate: int | None = None,
    pagination_hint: str | None = None,
    disable_ocr: bool | None = None,
) -> dict[str, Any]:
    """AI extraction from the page currently loaded in the session."""
    return _dump(_sdk.extract(
        data_instruction=data_instruction,
        json_schema=json_schema,
        model_size=model_size,
        vision=vision,
        feed_urls=feed_urls,
        viewport_min=viewport_min,
        viewport_max=viewport_max,
        paginate=paginate,
        pages_to_paginate=pages_to_paginate,
        pagination_hint=pagination_hint,
        disable_ocr=disable_ocr,
    ))


def act(
    instruction: str,
    *,
    max_steps: int = 10,
    model_size: str = "medium",
    extract_data: bool = False,
    data_instruction: str | None = None,
    json_schema: Any = None,
    viewport_min: int | None = None,
    viewport_max: int | None = None,
) -> dict[str, Any]:
    """Autonomous multi-step interaction, optionally collecting data."""
    return _dump(_sdk.act(
        instruction,
        max_steps,
        model_size=model_size,
        extract_data=extract_data or None,
        data_instruction=data_instruction,
        json_schema=json_schema,
        viewport_min=viewport_min,
        viewport_max=viewport_max,
    ))
