"""Complete mode with several accounts: the plan, and the union of the shares.

Each account collects its own share of the targets into its own complete-mode
checkpoint (``accounts/<n>/`` under the run's directory); the run's own
checkpoint keeps the plan and the merged dataset. Posts are handed out one
at a time by the runner; these helpers name the shares, give saved shares back
to their accounts, and merge what the shares collected. The runner drives the shares (``ScraperRun._run_shares``).
"""

from __future__ import annotations

import json
import sqlite3
from contextlib import closing
from pathlib import Path
from typing import Any

from .input_model import cookie_viewer_id
from .scrapers.base import Stats

__all__ = ["accounts_plan", "taken_urls", "assign", "merge_records", "merge_coverage",
           "stats_from_summary"]


def accounts_plan(jars: list[list[dict[str, Any]]]) -> list[dict[str, Any]]:
    """One share per account, named by the id in its cookies, so a resume can
    give it back to the same account whatever order the cookies come in.
    Posts are not dealt in advance: each share takes the next one when its
    browser is free (``ScraperRun._run_shares``)."""
    return [{"share": number, "directory": f"accounts/{number}", "viewerId": cookie_viewer_id(jar)}
            for number, jar in enumerate(jars, 1)]


def taken_urls(directory: Path) -> set[str]:
    """The posts a share's checkpoint already holds (canonical URLs)."""
    path = Path(directory) / "state.sqlite"
    if not path.is_file():
        return set()
    with closing(sqlite3.connect(path)) as db:
        try:
            return {row[0] for row in db.execute("SELECT id FROM targets")}
        except sqlite3.OperationalError:
            return set()


def assign(plan: list[dict[str, Any]], jars: list[list[dict[str, Any]]]) -> list[int | None]:
    """For each share of a saved plan, the index of the supplied account it
    belongs to, or None when that account was not supplied."""
    supplied = {cookie_viewer_id(jar): index for index, jar in enumerate(jars)}
    return [supplied.get(entry.get("viewerId")) for entry in plan]


def merge_records(state: Any, shares: list[tuple[int, Path]], order: dict[str, int]) -> int:
    """Replace the run's records with the union of the shares' records.

    Rows keep each share's identity key and field provenance. They are
    ordered by the position of their input URL in the run's input, then by
    share and collection order; a key two shares hold (an expanded owner)
    is kept once. Returns the number of merged records.
    """
    rows = []
    for number, directory in shares:
        path = Path(directory) / "state.sqlite"
        if not path.is_file():
            continue
        with closing(sqlite3.connect(path)) as db:
            for rowid, key, kind, scope, payload, fields in db.execute(
                    "SELECT rowid,key,kind,scope,payload,fields FROM records ORDER BY rowid"):
                try:
                    input_url = json.loads(payload).get("inputUrl")
                except (TypeError, ValueError, AttributeError):
                    input_url = None
                rows.append((order.get(input_url, len(order)), number, rowid,
                             key, kind, scope, payload, fields))
    rows.sort(key=lambda row: row[:3])
    with state.transaction():
        state.db.execute("DELETE FROM records")
        state.db.executemany("INSERT OR IGNORE INTO records VALUES(?,?,?,?,?)",
                             [row[3:] for row in rows])
    return state.count()


_FIELDS = ("pending", "unavailable_fields", "observed", "not_observed")


def merge_coverage(items: list[dict[str, Any] | None]) -> dict[str, Any] | None:
    """One coverage report for the run from the shares' reports: counts add
    up, and the run's traversal is exhausted only when every share's is."""
    items = [item for item in items if isinstance(item, dict)]
    if not items:
        return None
    merged = _merge(items)
    traversals = {item.get("traversal") for item in items}
    merged["traversal"] = (traversals.pop() if len(traversals) == 1
                           else "partial")
    fields = [item.get("fields") for item in items]
    merged["fields"] = next((value for value in _FIELDS if value in fields), None)
    return merged


def _merge(items: list[dict[str, Any]]) -> dict[str, Any]:
    merged: dict[str, Any] = {}
    for key in dict.fromkeys(k for item in items for k in item):
        values = [item[key] for item in items if key in item]
        if all(isinstance(v, bool) for v in values):
            merged[key] = any(values)
        elif all(isinstance(v, (int, float)) and not isinstance(v, bool) for v in values):
            merged[key] = sum(values)
        elif all(isinstance(v, dict) for v in values):
            merged[key] = _merge(values)
        else:
            merged[key] = next((v for v in values if v is not None), None)
    return merged


def stats_from_summary(data: dict[str, Any] | None, *, label: str) -> Stats:
    """A share's counters, read back from its ``OUTPUT`` summary."""
    data = data or {}
    stats = Stats()
    stats.targets_done = int(data.get("targetsSucceeded") or 0)
    stats.targets_failed = int(data.get("targetsFailed") or 0)
    stats.targets_partial = int(data.get("targetsPartial") or 0)
    stats.ai_calls = int(data.get("aiFallbackCalls") or 0)
    stats.date_filtered = int(data.get("dateFilteredComments") or 0)
    stats.fallbacks.update(data.get("fallbacksUsed") or {})
    stats.errors.update(data.get("errors") or {})
    for failure in data.get("failures") or []:
        failure = dict(failure)
        if failure.get("target") == "run":
            failure["target"] = f"run ({label})"
        stats.failures.append(failure)
    stats.pagination.extend(data.get("commentPagination") or [])
    return stats
