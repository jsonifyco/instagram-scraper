"""Bounded, bidirectional comment pagination with explicit coverage diagnostics."""
from __future__ import annotations

import json
from collections import deque
from typing import Any, Callable, Iterator


def node(value: dict[str, Any]) -> dict[str, Any]:
    return value["node"] if isinstance(value.get("node"), dict) else value


def identity(value: dict[str, Any]) -> str:
    value = node(value)
    pk = value.get("pk") or value.get("id")
    return str(pk) if pk is not None else "anonymous:" + json.dumps(value, sort_keys=True)


def walk(
    fetch: Callable[..., dict[str, Any]], *, replies: bool,
    report: dict[str, Any], max_pages: int, max_items: int | None,
    initial: list[dict[str, Any]] | None = None,
) -> Iterator[dict[str, Any]]:
    """Follow each advertised direction, without guessing or swapping cursors.

    A cycle or missing cursor is a partial result, never proof that every
    comment was collected. One overlapping page is allowed if its cursor moves.
    """
    report.update(pages=0, uniqueItems=0, duplicates=0, stopReason="running", issues=[])
    seen: set[str] = set()
    queue = deque([{}])
    requested: set[tuple] = set()
    stagnant: dict[str, int] = {}
    try:
        if max_items is not None and max_items <= 0:
            report["stopReason"] = "results_limit"
            return
        for item in initial or []:
            if not isinstance(item, dict):
                continue
            key = identity(item)
            if key in seen:
                continue
            seen.add(key)
            report["uniqueItems"] += 1
            if max_items is not None and report["uniqueItems"] >= max_items:
                report["stopReason"] = "results_limit"
            yield node(item)
            if max_items is not None and report["uniqueItems"] >= max_items:
                report["stopReason"] = "results_limit"
                return
        # Preview replies can already cover the declared thread, at no cost.
        expected = report.get("expectedItems")
        if initial and expected is not None and len(seen) >= expected:
            report["stopReason"] = "preview_covers_count"
            return

        while queue and report["pages"] < max_pages:
            params = queue.popleft()
            requested.add(tuple(params.items()))
            report["pages"] += 1
            body = fetch(**params)
            if not isinstance(body, dict):
                raise ValueError("Comment endpoint did not return a JSON object")
            if not replies and body.get("comment_count") is not None:
                report["reportedCommentCount"] = body["comment_count"]
            rows = body.get("child_comments" if replies else "comments") or []
            fresh = 0
            for item in rows:
                if not isinstance(item, dict):
                    continue
                key = identity(item)
                if key in seen:
                    report["duplicates"] += 1
                    continue
                seen.add(key)
                fresh += 1
                report["uniqueItems"] += 1
                if max_items is not None and report["uniqueItems"] >= max_items:
                    report["stopReason"] = "results_limit"
                yield node(item)
                if max_items is not None and report["uniqueItems"] >= max_items:
                    report["stopReason"] = "results_limit"
                    return

            # max_id loads the tail; min_id loads the head. The flags are
            # independent: has_more_comments=False does not end head loading.
            directions = (
                ("max_id", "next_max_child_cursor", "has_more_tail_child_comments"),
                ("min_id", "next_min_child_cursor", "has_more_head_child_comments"),
            ) if replies else (
                ("max_id", "next_max_id", "has_more_comments"),
                ("min_id", "next_min_id", "has_more_headload_comments"),
            )
            direction = next(iter(params), "initial")
            stagnant[direction] = stagnant.get(direction, 0) + 1 if not fresh else 0
            for param, cursor_key, flag in directions:
                # Old responses omit the head flag and expose just one cursor.
                more = body.get(flag)
                if (not replies and param == "max_id" and not body.get(cursor_key)
                        and body.get("next_min_id")
                        and "has_more_headload_comments" not in body):
                    # Legacy responses use has_more_comments for their only
                    # advertised (min_id) direction, not for a missing tail.
                    more = None
                if (not replies and param == "min_id" and more is None
                        and "has_more_headload_comments" not in body
                        and body.get("next_min_id")):
                    more = body.get("has_more_comments")
                if more is False:
                    continue
                cursor = body.get(cursor_key)
                if not cursor:
                    if more is True:
                        report["issues"].append("missing_cursor")
                    continue
                next_params = {param: str(cursor)}
                signature = tuple(next_params.items())
                if signature in requested:
                    report["issues"].append("cursor_cycle")
                elif next_params not in queue:
                    # An empty page ends this direction; in the earlier sample
                    # of 253 lists a follow-up did not recover one. Rows
                    # that are all duplicates are overlap, not exhaustion, so
                    # a cursor that keeps moving still earns one more page.
                    if not rows or stagnant[direction] >= 2:
                        report["issues"].append("no_progress")
                    else:
                        queue.append(next_params)
            if not rows and not queue and report.get("reportedCommentCount", 0):
                report["issues"].append("empty_page")
        if queue:
            report["issues"].append("page_limit")
        if replies and expected is not None and report["uniqueItems"] < expected:
            report["issues"].append("reported_count_gap")
        report["issues"] = list(dict.fromkeys(report["issues"]))
        report["stopReason"] = report["issues"][0] if report["issues"] else "source_exhausted"
    except Exception as exc:
        report["stopReason"] = "error"
        report["error"] = type(exc).__name__
        raise
    finally:
        if report["stopReason"] == "running":
            report["stopReason"] = "consumer_stopped"
