"""Shared helpers for reshaping Instagram JSON into the Apify output format."""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any, Iterable

__all__ = [
    "iso_timestamp", "extract_hashtags", "extract_mentions", "best_candidate",
    "all_candidates", "clean", "as_int", "as_str", "first", "dig",
    "MEDIA_TYPE_NAMES", "typename_to_type",
]

#: media_type in the private API -> the label Apify emits
MEDIA_TYPE_NAMES = {1: "Image", 2: "Video", 8: "Sidecar"}

#: __typename in the GraphQL shape -> the same labels
_TYPENAMES = {
    "GraphImage": "Image",
    "GraphVideo": "Video",
    "GraphSidecar": "Sidecar",
    "XDTGraphImage": "Image",
    "XDTGraphVideo": "Video",
    "XDTGraphSidecar": "Sidecar",
}

_HASHTAG_RE = re.compile(r"(?:^|[^\w&/])#([A-Za-z0-9_À-ɏЀ-ӿ.]+)")
_MENTION_RE = re.compile(r"(?:^|[^\w&/])@([A-Za-z0-9_.]+)")


def typename_to_type(typename: str | None) -> str | None:
    return _TYPENAMES.get(str(typename or ""))


def iso_timestamp(value: Any) -> str | None:
    """Unix seconds (or an ISO string) -> ``2025-01-31T12:00:00.000Z``."""
    if value in (None, "", 0):
        return None
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        if text.isdigit():
            value = int(text)
        else:
            try:
                parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
            except ValueError:
                return text
            return _format(parsed)
    try:
        moment = datetime.fromtimestamp(float(value), tz=timezone.utc)
    except (TypeError, ValueError, OSError, OverflowError):
        return None
    return _format(moment)


def _format(moment: datetime) -> str:
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    moment = moment.astimezone(timezone.utc)
    return moment.strftime("%Y-%m-%dT%H:%M:%S.") + f"{moment.microsecond // 1000:03d}Z"


def extract_hashtags(text: str | None) -> list[str]:
    """Hashtags in a caption, without the ``#``, in order, de-duplicated."""
    if not text:
        return []
    seen: dict[str, None] = {}
    for tag in _HASHTAG_RE.findall(text):
        cleaned = tag.rstrip(".")
        if cleaned:
            seen.setdefault(cleaned, None)
    return list(seen)


def extract_mentions(text: str | None) -> list[str]:
    """@-mentions in a caption, without the ``@``, in order, de-duplicated."""
    if not text:
        return []
    seen: dict[str, None] = {}
    for handle in _MENTION_RE.findall(text):
        cleaned = handle.rstrip(".")
        if cleaned:
            seen.setdefault(cleaned, None)
    return list(seen)


def best_candidate(versions: Any) -> str | None:
    """Highest-resolution URL out of an ``image_versions2`` / ``video_versions``."""
    candidates = _candidate_list(versions)
    if not candidates:
        return None
    best = max(
        candidates,
        key=lambda c: (int(c.get("width") or 0) * int(c.get("height") or 0)),
    )
    return best.get("url")


def all_candidates(versions: Any) -> list[str]:
    """Every distinct URL in a versions block, largest first."""
    candidates = _candidate_list(versions)
    ordered = sorted(
        candidates,
        key=lambda c: (int(c.get("width") or 0) * int(c.get("height") or 0)),
        reverse=True,
    )
    seen: dict[str, None] = {}
    for candidate in ordered:
        url = candidate.get("url")
        if url:
            seen.setdefault(str(url), None)
    return list(seen)


def _candidate_list(versions: Any) -> list[dict[str, Any]]:
    if isinstance(versions, dict):
        items = versions.get("candidates") or versions.get("additional_candidates") or []
    elif isinstance(versions, list):
        items = versions
    else:
        return []
    return [c for c in items if isinstance(c, dict) and c.get("url")]


def dig(source: Any, *path: str, default: Any = None) -> Any:
    """Safe nested lookup: ``dig(item, "caption", "text")``."""
    node = source
    for key in path:
        if not isinstance(node, dict):
            return default
        node = node.get(key)
        if node is None:
            return default
    return node


def first(values: Iterable[Any], default: Any = None) -> Any:
    for value in values:
        if value not in (None, "", []):
            return value
    return default


def as_int(value: Any, default: int | None = None) -> int | None:
    if value is None or isinstance(value, bool):
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def as_str(value: Any) -> str | None:
    if value in (None, ""):
        return None
    return str(value)


def clean(record: dict[str, Any]) -> dict[str, Any]:
    """Drop private bookkeeping keys (``_foo``) from a finished record."""
    return {k: v for k, v in record.items() if not k.startswith("_")}
