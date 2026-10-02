"""Parallel browsers: lanes, their shared target queue, and profile ids read
anonymously once for all of them.

A *lane* is one browser working through the run's targets: an anonymous
worker (``broConcurrency``) or one Instagram account (``sessionCookies`` plus
``sessionCookiesList``). Lanes take targets from one queue instead of fixed
round-robin buckets, so a faster lane takes more of the work, and a lane that
stops -- an account Instagram throttles or challenges, a browser that dies --
hands its unfinished target back to the others instead of dropping its share.
"""

from __future__ import annotations

import threading
import time
from collections import Counter, deque
from dataclasses import dataclass, field
from typing import Any

from .errors import ScraperError
from .urls import Target

__all__ = ["Lane", "LaneLost", "TargetQueue", "ProfileSeeds"]


class LaneLost(ScraperError):
    """The lane cannot go on: its account or browser is lost.

    Raised around the error that ended the lane, with the number of records
    the unfinished target had already produced (a target that produced none
    can be handed to another lane without duplicating rows).
    """

    def __init__(self, error: BaseException, produced: int) -> None:
        super().__init__(f"{type(error).__name__}: {error}")
        self.error = error
        self.produced = produced


@dataclass
class Lane:
    """One browser of a parallel run, and what it did."""

    label: str
    #: index into ``ScraperInput.account_jars()``; None for an anonymous lane
    account: int | None = None
    #: pending | running | finished | stopped | failed_to_start
    status: str = "pending"
    reason: str | None = None
    session_id: str | None = None
    viewer_id: str | None = None
    started_at: float | None = None
    ready_at: float | None = None
    finished_at: float | None = None
    targets_done: int = 0
    targets_partial: int = 0
    targets_failed: int = 0
    targets_handed_back: int = 0
    records: int = 0
    api_requests: int = 0
    #: VM swaps this lane's browser made behind a login wall
    ip_rotations: int = 0
    #: the lane's browser and its own counters (not part of the report)
    session: Any = field(default=None, repr=False)
    stats: Any = field(default=None, repr=False)

    def report(self) -> dict[str, Any]:
        def span(start: float | None, end: float | None) -> float | None:
            return round(end - start, 1) if start and end else None
        return {
            "label": self.label,
            "kind": "account" if self.account is not None else "anonymous",
            "viewerId": self.viewer_id,
            "sessionId": self.session_id,
            "status": self.status,
            "reason": self.reason,
            "targetsSucceeded": self.targets_done,
            "targetsPartial": self.targets_partial,
            "targetsFailed": self.targets_failed,
            "targetsHandedBack": self.targets_handed_back,
            "records": self.records,
            "apiRequests": self.api_requests,
            "ipRotations": self.ip_rotations,
            "startupSeconds": span(self.started_at, self.ready_at),
            "workSeconds": span(self.ready_at, self.finished_at),
            "seconds": span(self.started_at, self.finished_at),
        }


class TargetQueue:
    """Targets waiting for a lane, shared by every lane of the run.

    ``take`` never waits: it returns ``None`` as soon as nothing is queued
    (or after ``close``), so a lane with no work left stops its VM at once
    instead of billing while it waits for a neighbour that might hand a
    target back. A handed-back target goes to a lane that is still working;
    when none is left, it stays queued and the run reports it as not
    scraped. A target is handed back at most ``max_attempts - 1`` times, so
    a target that brings down every browser it meets cannot take the whole
    run with it.
    """

    def __init__(self, targets: list[Target], *, max_attempts: int = 2) -> None:
        self._items: deque[tuple[int, Target]] = deque(enumerate(targets))
        self._attempts: Counter[int] = Counter()
        self._closed = False
        self._lock = threading.Lock()
        self.max_attempts = max_attempts

    def take(self) -> tuple[int, Target] | None:
        with self._lock:
            if self._closed or not self._items:
                return None
            item = self._items.popleft()
            self._attempts[item[0]] += 1
            return item

    def finish(self, item: tuple[int, Target], *, hand_back: bool = False) -> bool:
        """Release a taken target; ``hand_back`` queues it again for another
        lane. Returns True when it was queued again."""
        with self._lock:
            queued = (hand_back and not self._closed
                      and self._attempts[item[0]] < self.max_attempts)
            if queued:
                self._items.appendleft(item)
            return queued

    def close(self) -> None:
        """No lane takes another target (a run-wide stop, e.g. the budget)."""
        with self._lock:
            self._closed = True

    def drain(self) -> list[tuple[int, Target]]:
        """Whatever no lane took, in queue order."""
        with self._lock:
            items, self._items = list(self._items), deque()
            return items


class ProfileSeeds:
    """Profile ids and snapshots read anonymously, before any account's
    cookies went into its browser, shared by the run's account lanes.

    They are not tied to a viewer, so any account may use them. Each lane
    claims the usernames it reads, so two lanes never read the same profile;
    a lane that needs a profile another lane is still reading waits for it
    (bounded) instead of reading it again with its account.
    """

    def __init__(self) -> None:
        self._cond = threading.Condition()
        self._records: dict[str, dict[str, Any]] = {}
        self._sources: dict[str, str] = {}
        self._reading: set[str] = set()
        self._settled: set[str] = set()

    def claim(self, key: str) -> bool:
        with self._cond:
            if key in self._reading or key in self._settled:
                return False
            self._reading.add(key)
            return True

    def settle(self, key: str, record: dict[str, Any] | None, source: str | None) -> None:
        with self._cond:
            self._reading.discard(key)
            self._settled.add(key)
            if record and record.get("id"):
                self._records[key] = record
                self._sources[key] = source or "web_profile_info"
            self._cond.notify_all()

    def get(self, key: str, *, wait: float = 0.0) -> tuple[dict[str, Any], str] | None:
        deadline = time.monotonic() + wait
        with self._cond:
            while key in self._reading:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                self._cond.wait(remaining)
            record = self._records.get(key)
            return (record, self._sources[key]) if record else None

    def snapshot(self) -> tuple[dict[str, dict[str, Any]], dict[str, str]]:
        with self._cond:
            return dict(self._records), dict(self._sources)
