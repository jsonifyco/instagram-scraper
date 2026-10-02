"""One shared budget for a run: managed Instagram requests, elapsed time and
the VM swaps anonymous browsers may make behind a login wall."""
from __future__ import annotations
import threading
import time
from contextlib import contextmanager
from .errors import FatalError


class BudgetExceeded(FatalError):
    def __init__(self, reason: str):
        self.reason = reason
        super().__init__(f"Run budget reached: {reason}; saved work can be resumed")


class RunBudget:
    def __init__(self, max_requests=None, max_seconds=None, *, clock=time.monotonic,
                 max_enrichment_requests=None, max_ip_rotations=0):
        self.max_requests, self.max_seconds = max_requests, max_seconds
        self.clock = clock
        self.started = clock()
        self.requests = 0
        self.initial_reads = 0
        self.retry_reads = 0
        self.reason = None
        self.lock = threading.Lock()
        self.max_enrichment_requests = max_enrichment_requests
        self.enrichment_requests = 0
        #: ``maxIpRotations``: VM swaps all anonymous browsers of the run may
        #: make together, and how many they made
        self.max_ip_rotations = max_ip_rotations
        self.ip_rotations = 0
        # Browsers of a parallel run share this budget; whether a read is an
        # enrichment read belongs to the thread that makes it.
        self._local = threading.local()

    @property
    def enriching(self):
        return getattr(self._local, "enriching", False)

    @enriching.setter
    def enriching(self, value):
        self._local.enriching = bool(value)

    @contextmanager
    def enrichment(self):
        previous = self.enriching
        self.enriching = True
        try:
            yield
        finally:
            self.enriching = previous

    def check(self):
        if self.max_seconds is not None and self.clock() - self.started >= self.max_seconds:
            self.reason = "maxRunSeconds"
            raise BudgetExceeded(self.reason)

    def take(self, *, retry=False):
        with self.lock:
            self.check()
            if (self.enriching and self.max_enrichment_requests is not None
                    and self.enrichment_requests >= self.max_enrichment_requests):
                raise BudgetExceeded("maxEnrichmentRequests")
            if self.max_requests is not None and self.requests >= self.max_requests:
                self.reason = "maxApiRequests"
                raise BudgetExceeded(self.reason)
            self.requests += 1
            if retry:
                self.retry_reads += 1
            else:
                self.initial_reads += 1
            if self.enriching:
                self.enrichment_requests += 1

    def remaining(self):
        self.check()
        return None if self.max_seconds is None else max(0.1, self.max_seconds - (self.clock() - self.started))

    def take_ip_rotation(self):
        """One VM swap out of ``maxIpRotations``; False when none is left."""
        with self.lock:
            if self.ip_rotations >= self.max_ip_rotations:
                return False
            self.ip_rotations += 1
            return True

    def summary(self):
        return {"managedApiRequests": self.requests,
                "initialReads": self.initial_reads,
                "retryReads": self.retry_reads,
                "enrichmentApiRequests": self.enrichment_requests,
                "elapsedSeconds": round(self.clock() - self.started, 3),
                "ipRotations": self.ip_rotations,
                "maxIpRotations": self.max_ip_rotations,
                "stopReason": self.reason}


class ShareBudget(RunBudget):
    """One share's view of a budget that several browsers share.

    The limits and the clock are the run's: every request is taken from the
    run budget as well, which enforces ``maxApiRequests`` (and the enrichment
    cap) across all shares. The counts are the share's own, so its summary
    says what this share read.
    """

    def __init__(self, run: RunBudget):
        super().__init__(clock=run.clock)
        self.run = run
        self.started = run.started
        self.max_requests, self.max_seconds = run.max_requests, run.max_seconds
        self.max_enrichment_requests = run.max_enrichment_requests

    def _run_limit(self, action, *args, **kwargs):
        try:
            return action(*args, **kwargs)
        except BudgetExceeded as exc:
            self.reason = exc.reason
            raise

    def check(self):
        self._run_limit(self.run.check)

    def remaining(self):
        return self._run_limit(self.run.remaining)

    def take(self, *, retry=False):
        if self.enriching:
            with self.run.enrichment():
                self._run_limit(self.run.take, retry=retry)
        else:
            self._run_limit(self.run.take, retry=retry)
        with self.lock:
            self.requests += 1
            if retry:
                self.retry_reads += 1
            else:
                self.initial_reads += 1
            if self.enriching:
                self.enrichment_requests += 1
