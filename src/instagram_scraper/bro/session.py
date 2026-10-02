"""A managed getbro browser session.

Wraps :class:`~instagram_scraper.bro.client.BroClient` with the lifecycle the
scraper needs:

* lazy creation, so a session is only billed once it is actually used
* optional re-creation for standalone anonymous callers; complete-mode runs
  disable it, and cookie-bearing sessions stop rather than moving the account
* a `run()` helper that returns decoded step payloads instead of raw envelopes
* guaranteed teardown via the context-manager protocol
"""

from __future__ import annotations

import logging
import threading
from types import TracebackType
from typing import Any, Callable, Iterable

from ..errors import (BroCommandError, BroCommandOutcomeUnknown, BroError,
                      BroHTTPError, BroPayloadError, BroSessionError,
                      BroTimeoutError, FatalError)
from . import commands as cmd
from .client import DEAD_STATUSES, BroClient, step_data, untag_steps

log = logging.getLogger(__name__)


class BroSession:
    """One remote browser, reusable across many command batches."""

    def __init__(
        self,
        client: BroClient,
        *,
        session_kwargs: dict[str, Any] | None = None,
        session_timeout: float = 3600.0,
        command_timeout: float = 300.0,
        recovery_timeout: float = 120.0,
        cookies: list[dict[str, Any]] | None = None,
        on_start: Callable[["BroSession"], None] | None = None,
        label: str = "session",
    ) -> None:
        self.client = client
        self.session_kwargs = dict(session_kwargs or {})
        self.session_timeout = session_timeout
        self.command_timeout = command_timeout
        self.recovery_timeout = recovery_timeout
        self.cookies = list(cookies or [])
        self.on_start = on_start
        self.label = label

        self.session_id: str | None = None
        self.info: dict[str, Any] = {}
        self.restarts = 0
        #: cost of VMs this session already discarded, so a run that rotated
        #: its exit IP several times still reports what it actually spent
        self.retired_cost = 0.0
        self.retired_breakdown: dict[str, float] = {}
        self._lock = threading.RLock()
        self._closed = False
        self.no_restart = False
        self.time_remaining = None

    # ------------------------------------------------------------ lifecycle --

    @property
    def is_open(self) -> bool:
        return self.session_id is not None and not self._closed

    def start(self) -> str:
        """Create the remote session (idempotent) and return its id."""
        with self._lock:
            if self.session_id:
                return self.session_id
            payload = dict(self.session_kwargs)
            payload["session_timeout"] = self.session_timeout
            if self.cookies:
                payload["cookies"] = self.cookies
            log.info("[%s] creating getbro session (proxy=%s %s/%s)", self.label,
                     payload.get("enable_proxy"), payload.get("proxy_tier", "-"),
                     payload.get("country", "-"))
            self.session_id = self.client.create_session(**payload)
            self.info = self.client.wait_until_idle(self.session_id)
            self._closed = False
            log.info("[%s] session %s ready", self.label, self.session_id)
        if self.on_start:
            self.on_start(self)
        return self.session_id

    def restart(self) -> str:
        """Throw away the current VM and boot a fresh one."""
        if self.cookies or self.no_restart:
            raise FatalError("Authenticated browser interrupted; stopping without "
                             "replaying cookies on another browser/IP")
        with self._lock:
            old = self.session_id
            if old:
                self._bank_cost()
                try:
                    self.client.stop_session(old)
                except BroError:
                    pass
            self.session_id = None
            self.restarts += 1
        log.warning("[%s] restarting session (was %s, restart #%d)",
                    self.label, old, self.restarts)
        return self.start()

    def close(self) -> None:
        """Stop the remote session; safe to call more than once."""
        with self._lock:
            if not self.session_id or self._closed:
                self._closed = True
                return
            session_id = self.session_id
            self._closed = True
        try:
            self.client.stop_session(session_id)
            log.info("[%s] session %s stopped", self.label, session_id)
        except BroError as exc:
            log.warning("[%s] could not stop session %s: %s", self.label, session_id, exc)

    def __enter__(self) -> "BroSession":
        self.start()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()

    # ------------------------------------------------------------- commands --

    def run(
        self,
        commands: dict[str, Any] | Iterable[dict[str, Any]],
        *,
        timeout: float | None = None,
        retries: int = 1,
        raise_on_failure: bool = True,
        on_event=None,
        idempotent: bool | None = None,
    ) -> list[Any]:
        """Execute a batch and return the decoded payload of every step.

        A dead anonymous session may be replaced. A cookie-bearing session
        raises FatalError instead of replaying cookies on another VM/IP.
        ``idempotent`` is forwarded to the client (see ``BroClient.execute``).
        """
        batch = [commands] if isinstance(commands, dict) else list(commands)
        attempt = 0
        while True:
            attempt += 1
            session_id = self.start()
            try:
                effective_timeout = timeout or self.command_timeout
                if self.time_remaining:
                    remaining = self.time_remaining()
                    if remaining is not None:
                        effective_timeout = min(effective_timeout, remaining)
                steps = self.client.execute(
                    session_id, batch,
                    timeout=effective_timeout,
                    recovery_timeout=self.recovery_timeout,
                    raise_on_failure=raise_on_failure,
                    on_event=on_event,
                    idempotent=idempotent,
                )
                try:
                    return [step_data(step) for step in steps]
                except BroPayloadError as exc:
                    # The browser command is already done. Re-read its envelope
                    # and retry only the offloaded object download.
                    if not exc.command_id:
                        raise
                    result = self.client.await_command(
                        session_id, exc.command_id, timeout=0,
                        recovery_timeout=self.recovery_timeout,
                        on_event=on_event)
                    refreshed = untag_steps(list((result.get("response") or {}).get("commands") or []))
                    for step in refreshed:
                        step["_bro_session_id"] = session_id
                        step["_bro_command_id"] = exc.command_id
                    return [step_data(step) for step in refreshed]
            except BroCommandOutcomeUnknown:
                # A pending command cannot coexist safely with a new one, and
                # a completed command must not be replayed for its payload.
                raise
            except (BroSessionError, BroTimeoutError) as exc:
                if attempt > retries:
                    raise
                if self.cookies or self.no_restart:
                    raise
                log.warning("[%s] %s -- restarting and retrying", self.label, exc)
                self.restart()
            except BroCommandError as exc:
                if attempt > retries or not _is_session_fault(exc):
                    raise
                if self.cookies or self.no_restart:
                    raise
                log.warning("[%s] session fault (%s) -- restarting", self.label, exc.name)
                self.restart()

    def run_one(self, command: dict[str, Any], **kwargs: Any) -> Any:
        """Execute a single command and return its payload."""
        results = self.run(command, **kwargs)
        return results[0] if results else None

    # ------------------------------------------------------------ shortcuts --

    def attach(self, session_id: str) -> bool:
        """Attach to an existing idle/running VM without injecting cookies."""
        try:
            info = self.client.get_session(session_id)
        except BroHTTPError as exc:
            if exc.status == 404:
                return False
            raise
        if info.get("status") not in ("idle", "busy", "running", "ready"):
            return False
        self.session_id, self.info, self._closed = session_id, info, False
        self.no_restart = True
        return True

    def stop_and_sync(self, *, attempts: int = 5, billing_reads: int = 3) -> dict[str, Any]:
        """Read billing after a confirmed stop; bound status polling.

        A stopped session's snapshot can arrive without its ``billing`` block
        while the API is slow (seen 17 September: Admin API showed $0.31 for
        a run whose stop-time read said nothing). The read is repeated up to
        ``billing_reads`` times; ``billingSynced`` says whether a snapshot
        with billing was ever seen, so a missing block is reported as
        unknown rather than as a free run.
        """
        import time
        self.close()
        for attempt in range(attempts):
            if not self.session_id:
                return {"confirmedStopped": True, "total_billed": 0.0, "billingSynced": True}
            self.info = self.client.get_session(self.session_id)
            if str(self.info.get("status") or "") in DEAD_STATUSES:
                synced = self._billing_present()
                for _ in range(billing_reads - 1):
                    if synced:
                        break
                    time.sleep(2)
                    try:
                        self.info = self.client.get_session(self.session_id)
                    except BroError:
                        continue
                    synced = self._billing_present()
                return {**self.billing(refresh=False), "confirmedStopped": True,
                        "billingSynced": synced}
            if attempt + 1 < attempts:
                try:
                    self.client.stop_session(self.session_id)
                except BroError:
                    pass
                time.sleep(1)
        return {**self.billing(refresh=False), "confirmedStopped": False,
                "billingSynced": self._billing_present()}

    def _billing_present(self) -> bool:
        """True when the last session snapshot carried a billing block."""
        billing = self.info.get("billing") if isinstance(self.info, dict) else None
        return isinstance(billing, dict) and "total_billed" in billing

    def goto(self, url: str, *, wait: float = 2.0, timeout: float | None = None) -> str:
        """Navigate and return the URL actually landed on."""
        batch = [cmd.open_url(url)]
        if wait:
            batch.append(cmd.sleep(wait))
        batch.append(cmd.get_url())
        results = self.run(batch, timeout=timeout)
        final = results[-1] if results else None
        return str((final or {}).get("url") or url)

    def js(self, code: str, out_type: str | None = "str") -> Any:
        """Evaluate JavaScript on the current page and return its result."""
        payload = self.run_one(cmd.run_js(code, out_type=out_type))
        return (payload or {}).get("result")

    def current_url(self) -> str:
        payload = self.run_one(cmd.get_url())
        return str((payload or {}).get("url") or "")

    def inject_cookies(self, cookies: list[dict[str, Any]]) -> None:
        if cookies:
            self.run(cmd.inject_cookies(cookies))

    def dump_cookies(self) -> list[dict[str, Any]]:
        payload = self.run_one(cmd.dump_cookies())
        return list((payload or {}).get("cookies") or [])

    # -------------------------------------------------------------- billing --

    def billing(self, *, refresh: bool = True) -> dict[str, Any]:
        """Billing for this session, including the VMs it already discarded.

        ``refresh=False`` reads the last snapshot instead of asking again.
        """
        current: dict[str, Any] = {}
        if self.session_id:
            if refresh:
                try:
                    self.info = self.client.get_session(self.session_id)
                except BroError:
                    pass
            current = dict((self.info or {}).get("billing") or {})

        breakdown = dict(self.retired_breakdown)
        for resource, cost in (current.get("breakdown") or {}).items():
            breakdown[resource] = breakdown.get(resource, 0.0) + float(cost or 0.0)

        return {
            "last_synced_at": current.get("last_synced_at"),
            "total_billed": self.retired_cost + float(current.get("total_billed") or 0.0),
            "breakdown": breakdown,
            "restarts": self.restarts,
        }

    def total_billed(self) -> float:
        return float(self.billing().get("total_billed") or 0.0)

    def _bank_cost(self) -> None:
        """Fold the current VM's cost into the retired totals before dropping it."""
        if not self.session_id:
            return
        try:
            snapshot = dict(self.client.get_session(self.session_id).get("billing") or {})
        except BroError:
            return
        self.retired_cost += float(snapshot.get("total_billed") or 0.0)
        for resource, cost in (snapshot.get("breakdown") or {}).items():
            self.retired_breakdown[resource] = (
                self.retired_breakdown.get(resource, 0.0) + float(cost or 0.0)
            )


_SESSION_FAULTS = (
    "sessionnotfound", "sessionexpired", "sessionterminated", "sessionstopped",
    "browserclosed", "targetclosed", "connectionclosed", "sessiongone",
    "workerlost", "notidle",
)


def _is_session_fault(exc: BroCommandError) -> bool:
    """True when a command failed because the VM is gone, not because of us."""
    blob = f"{exc.name}{exc.message}".lower().replace("_", "").replace(" ", "")
    return any(marker in blob for marker in _SESSION_FAULTS)
