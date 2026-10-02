"""Transport-neutral getbro client: recovery discipline and payload helpers.

The wire is the official ``bro-api-sdk`` package
(:class:`~instagram_scraper.bro.sdk_client.SdkBroClient`, the only concrete
client). What lives here is everything above the wire that must behave the
same whatever carries the bytes:

* the session and command status vocabularies
* ``execute`` / ``await_command`` / ``_recover_command`` -- a batch with a
  known command id is observed until it settles; after a lost submit only an
  idempotent batch may be sent again, never an ambiguous mutation. An outcome
  that never settles becomes ``BroCommandOutcomeUnknown`` for checkpointing.
* ``wait_until_idle`` and ``check_balance`` on top of the primitives
* the offloaded-payload download (``step_data`` / ``last_data``)

The primitives themselves (``create_session``, ``get_session``,
``stop_session``, ``submit``, ``get_command``, ``get_user``) are abstract
here. Tests that exercise the recovery loops mock them on a bare
:class:`BroClient`; production always instantiates the SDK subclass via
:func:`instagram_scraper.bro.make_client`.

Note on the User-Agent: ``api.getbro.ws`` sits behind a WAF. The SDK client
sets a browser-like User-Agent for API calls; offloaded payload downloads use
``requests`` with the same User-Agent. The older REST transport is not used
by production.
"""

from __future__ import annotations

import json
import logging
import time
import uuid
from typing import Any, Iterable

import requests

from ..errors import (
    BroAuthError,
    BroCommandError,
    BroHTTPError,
    BroCommandOutcomeUnknown,
    BroPayloadError,
    BroSessionError,
    BroSubmitNotAccepted,
    BroTimeoutError,
)

log = logging.getLogger(__name__)

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36 instagram-scraper/1.2"
)

#: statuses a session can be in once it is usable
LIVE_STATUSES = frozenset({"idle", "busy"})
#: statuses a session passes through before it is usable
BOOTING_STATUSES = frozenset({"queued", "initializing"})
#: statuses from which a session will never recover
DEAD_STATUSES = frozenset({"terminated", "stopped", "failed", "cancelled"})
#: statuses from which a command will never move on
TERMINAL_COMMAND_STATUSES = frozenset({"done", "failed", "stopped", "cancelled"})
#: commands whose repetition changes nothing on the page or the account:
#: reads, dumps, and navigation to a URL (landing twice on the same page is
#: what a reload is; measured 18 September: a lost ``open_url`` submit that
#: the API never registered). ``fetch_json`` counts only as a GET;
#: ``run_js`` never counts on its own -- a caller that knows its script is
#: idempotent (the page-fetch slot scripts) says so explicitly.
READ_ONLY_COMMANDS = frozenset({
    "get_url", "get_html", "get_screenshot", "sleep", "dump_cookies",
    "dump_local_storage", "dump_console_logs", "dump_har_logs",
    "open_url", "refresh",
})


#: prefix of the identity a batch carries in its last step (see ``tag_batch``)
BATCH_TAG_PREFIX = "igs-batch:"


def tag_batch(batch: list[dict[str, Any]], nonce: str) -> list[dict[str, Any]]:
    """``batch`` plus one trailing ``run_js`` step that evaluates to the
    batch's identity. It is the only thing the public API echoes back that
    this client chose: a command found in the session's list after a lost
    submit is ours when its executed steps end with this value, and not
    ours otherwise -- names and timestamps alone cannot tell two batches
    apart. The step runs nothing but a string literal."""
    return list(batch) + [{"command": "run_js",
                           "params": {"js_code": f"{BATCH_TAG_PREFIX}{nonce}".join(("'", "'")),
                                      "out_type": "str"}}]


def batch_tag_of(steps: list[dict[str, Any]]) -> str | None:
    """The identity a completed command's steps carry, if any."""
    if not steps:
        return None
    last = steps[-1] if isinstance(steps[-1], dict) else {}
    data = last.get("data") if isinstance(last.get("data"), dict) else {}
    value = data.get("result")
    if last.get("command") == "run_js" and isinstance(value, str) and value.startswith(BATCH_TAG_PREFIX):
        return value[len(BATCH_TAG_PREFIX):]
    return None


def untag_steps(steps: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """``steps`` without the trailing identity step."""
    return steps[:-1] if batch_tag_of(steps) is not None else steps


def read_only_batch(batch: list[dict[str, Any]]) -> bool:
    """True when re-sending ``batch`` could not act twice on anything."""
    for command in batch:
        name = str(command.get("command") or "")
        if name in READ_ONLY_COMMANDS:
            continue
        if name == "fetch_json":
            params = command.get("params") or {}
            if str(params.get("method") or "GET").upper() == "GET":
                continue
        return False
    return bool(batch)


class BroClient:
    """Recovery loops and metrics shared by every getbro transport.

    Subclasses implement the wire primitives; this class never issues an
    HTTP request of its own.
    """

    def __init__(
        self,
        api_key: str,
        *,
        base_url: str = "https://api.getbro.ws",
        timeout: float = 120.0,
    ) -> None:
        if not api_key:
            raise BroAuthError("getbro API key is empty")
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        #: optional callable returning the seconds left in the run budget;
        #: bounds every observation and recovery loop
        self.time_remaining = None
        self.management_requests = 0
        self.recovery_seconds = 0.0
        self.recovered_commands = 0
        self.unknown_commands = 0
        self.drop_observation_once = False
        self.observation_faults_injected = 0
        #: idempotent batches resent after bounded command-list reconciliation;
        #: a temporarily empty listing is not proof of non-acceptance
        self.resent_submits = 0

    # ---------------------------------------------------------- primitives --

    def get_user(self) -> dict[str, Any]:
        """Account snapshot: balance, tier, concurrent-session allowance."""
        raise NotImplementedError("wire primitive; use the SDK client")

    def create_session(self, **kwargs: Any) -> str:
        """Create a session and return its id."""
        raise NotImplementedError("wire primitive; use the SDK client")

    def get_session(self, session_id: str) -> dict[str, Any]:
        raise NotImplementedError("wire primitive; use the SDK client")

    def get_session_once(self, session_id: str, *, timeout: float = 15) -> dict[str, Any]:
        """One session read with no retries of our own (recovery loops)."""
        return self.get_session(session_id)

    def stop_session(self, session_id: str) -> dict[str, Any]:
        raise NotImplementedError("wire primitive; use the SDK client")

    def submit(self, session_id: str, commands: list[dict[str, Any]], *,
               idempotent: bool = False, nonce: str | None = None) -> str:
        """Queue a command batch, returning its command id. ``idempotent``
        says the batch may be sent again should its acceptance be in doubt;
        ``nonce`` is the identity its last step carries (``tag_batch``)."""
        raise NotImplementedError("wire primitive; use the SDK client")

    def get_command(self, session_id: str, command_id: str, *, timeout: float = 30) -> dict[str, Any]:
        raise NotImplementedError("wire primitive; use the SDK client")

    # ------------------------------------------------------------- account --

    def check_balance(self, minimum: float = 1.0) -> float:
        """Return the balance, raising if it is below getbro's launch minimum."""
        user = self.get_user()
        balance = float(user.get("balance") or 0.0)
        if balance < minimum:
            raise BroAuthError(
                f"getbro balance is ${balance:.2f}; at least ${minimum:.2f} is "
                "required to launch a browser session"
            )
        return balance

    # ------------------------------------------------------------ sessions --

    def wait_until_idle(self, session_id: str, *, timeout: float = 240.0) -> dict[str, Any]:
        """Block until the session is usable.

        ``GET /v1/sessions/{id}`` long-polls for up to 15s while the session is
        queued/initializing, so this loop is cheap.
        """
        deadline = time.monotonic() + timeout
        info: dict[str, Any] = {}
        while time.monotonic() < deadline:
            info = self.get_session(session_id)
            status = str(info.get("status") or "")
            if status in LIVE_STATUSES:
                return info
            if status in DEAD_STATUSES:
                raise BroSessionError(
                    f"session {session_id} became {status}: "
                    f"{info.get('error_name')} {info.get('error_message')}",
                    session_id=session_id, phase="create", last_status=status)
            time.sleep(1.5)
        raise BroSessionError(
            f"session {session_id} did not become idle within {timeout:.0f}s "
            f"(last status: {info.get('status')})",
            session_id=session_id, phase="create", last_status=str(info.get("status") or ""))

    # ------------------------------------------------------------ commands --

    @staticmethod
    def _event(callback, state: str, **fields: Any) -> None:
        if callback:
            callback(state, fields)

    def _budget(self, timeout: float) -> float:
        """``timeout`` clamped to what is left of the run budget."""
        remaining = self.time_remaining() if self.time_remaining else None
        return max(0.0, min(timeout, remaining) if remaining is not None else timeout)

    def _recovery_deadline(self, timeout: float) -> float:
        return time.monotonic() + self._budget(timeout)

    def _recover_command(self, session_id: str, command_id: str, *, timeout: float,
                         last_status: str | None, on_event=None) -> dict[str, Any]:
        """Resolve one submitted command without ever submitting it again."""
        started = time.monotonic()
        deadline = self._recovery_deadline(timeout)
        self._event(on_event, "recovering", phase="observe", last_status=last_status)
        result: dict[str, Any] = {}
        session_status: str | None = None
        while time.monotonic() < deadline:
            remaining = max(0.1, deadline - time.monotonic())
            try:
                result = self.get_command(session_id, command_id, timeout=min(30, remaining))
            except (BroHTTPError, BroTimeoutError):
                result = {}
            status = str(result.get("status") or last_status or "")
            if status in TERMINAL_COMMAND_STATUSES:
                self.recovered_commands += 1
                self.recovery_seconds += time.monotonic() - started
                self._event(on_event, "recovered", phase="observe", last_status=status)
                return result
            if status:
                last_status = status
            if time.monotonic() >= deadline:
                break
            remaining = max(0.1, deadline - time.monotonic())
            try:
                session = self.get_session_once(session_id, timeout=min(15, remaining))
                session_status = str(session.get("status") or "")
                if session_status in DEAD_STATUSES:
                    self.recovery_seconds += time.monotonic() - started
                    raise BroSessionError(
                        f"browser session is {session_status}", session_id=session_id,
                        command_id=command_id, phase="session_check", last_status=last_status)
            except BroHTTPError:
                # A session lookup failure does not prove that the VM died.
                pass
            delay = min(1.0, max(0.0, deadline - time.monotonic()))
            if delay:
                time.sleep(delay)
        self.recovery_seconds += time.monotonic() - started
        self.unknown_commands += 1
        self._event(on_event, "outcome_unknown", phase="recover", last_status=last_status)
        raise BroCommandOutcomeUnknown(
            "command outcome remains unknown after recovery budget",
            session_id=session_id, command_id=command_id, phase="recover",
            last_status=last_status or session_status)

    def await_command(
        self, session_id: str, command_id: str, *, timeout: float = 300.0,
        recovery_timeout: float = 120.0, on_event=None,
    ) -> dict[str, Any]:
        """Poll a command until it reaches a terminal status.

        A command lookup can briefly return 404 even while its session remains
        live (observed after roughly ten minutes). Re-check the session and
        retry the read only; the command is never submitted a second time.
        """
        deadline = time.monotonic() + timeout
        result: dict[str, Any] = {}
        while time.monotonic() < deadline:
            observation_started = time.monotonic()
            try:
                result = self.get_command(session_id, command_id)
            except BroHTTPError as exc:
                if exc.status in (0, 404, 429) or exc.status >= 500:
                    episode_remaining = max(
                        0.0, recovery_timeout - (time.monotonic() - observation_started))
                    return self._recover_command(session_id, command_id, timeout=episode_remaining,
                                                 # The failed observation belongs to this episode.
                                                 last_status=str(result.get("status") or "") or None,
                                                 on_event=on_event)
                raise
            if self.drop_observation_once and not self.observation_faults_injected and on_event:
                # Verification hook: model a lost successful observation after
                # submit. Recovery must use this command ID and never resubmit.
                self.observation_faults_injected = 1
                self._event(on_event, "observation_lost", phase="observe",
                            last_status=None)
                return self._recover_command(session_id, command_id,
                                             timeout=recovery_timeout,
                                             last_status=None, on_event=on_event)
            status = str(result.get("status") or "")
            self._event(on_event, "observed", phase="observe", last_status=status)
            if status in TERMINAL_COMMAND_STATUSES:
                return result
            time.sleep(0.5)
        return self._recover_command(session_id, command_id, timeout=recovery_timeout,
                                     last_status=str(result.get("status") or "") or None,
                                     on_event=on_event)

    def execute(
        self,
        session_id: str,
        commands: dict[str, Any] | Iterable[dict[str, Any]],
        *,
        timeout: float = 300.0,
        recovery_timeout: float = 120.0,
        raise_on_failure: bool = True,
        on_event=None,
        idempotent: bool | None = None,
    ) -> list[dict[str, Any]]:
        """Run a batch and return its per-step results.

        Each returned step is the raw getbro step object; use
        :func:`step_data` to read its payload (which transparently downloads
        offloaded results). ``idempotent`` (default: derived with
        :func:`read_only_batch`) is what allows a lost submit to be sent
        again once the API has shown it created nothing; a batch that could
        act twice (a click, a scroll, an unknown script) is never re-sent and
        ends as ``BroCommandOutcomeUnknown`` instead.
        """
        batch = [commands] if isinstance(commands, dict) else list(commands)
        if not batch:
            return []
        if idempotent is None:
            idempotent = read_only_batch(batch)
        nonce = uuid.uuid4().hex[:16]
        tagged = tag_batch(batch, nonce)
        self._event(on_event, "submitting", phase="submit")
        try:
            try:
                command_id = self.submit(session_id, tagged, idempotent=idempotent, nonce=nonce)
            except BroSubmitNotAccepted as first:
                # No command of ours showed up after the loss and the batch
                # is read-only: sending it once more cannot act twice on
                # anything, whatever the first submit's fate. That safety
                # comes from idempotency alone, not from the empty snapshots.
                # A second loss stays unknown -- never a loop.
                self.resent_submits += 1
                self._event(on_event, "resubmitting", phase="submit", last_status="not_accepted")
                try:
                    command_id = self.submit(session_id, tagged, idempotent=idempotent, nonce=nonce)
                except BroSubmitNotAccepted as second:
                    raise BroCommandOutcomeUnknown(
                        "two submit responses were lost; refusing a third send",
                        session_id=session_id, phase="submit", last_status="unknown") from second
                log.warning("[%s] idempotent batch re-sent after a lost submit; "
                            "the first attempt remains unconfirmed (%s)",
                            session_id[:8], first)
        except BroHTTPError as exc:
            raise BroCommandOutcomeUnknown(
                "command submission response was lost; refusing blind replay",
                session_id=session_id, phase="submit", last_status="unknown") from exc
        except BroCommandError as exc:
            if exc.name != "NoCommandId":
                raise
            raise BroCommandOutcomeUnknown(
                "getbro accepted a submit request but returned no command ID",
                session_id=session_id, phase="submit", last_status="unknown") from exc
        self._event(on_event, "submitted", command_id=command_id, phase="observe")
        result = self.await_command(session_id, command_id, timeout=timeout,
                                    recovery_timeout=recovery_timeout, on_event=on_event)
        steps = untag_steps(list((result.get("response") or {}).get("commands") or []))

        if raise_on_failure and str(result.get("status")) != "done":
            failed = next((s for s in steps if not s.get("success")), {})
            raise BroCommandError(
                str(failed.get("error_name") or result.get("error_name") or "CommandFailed"),
                str(failed.get("error_message") or result.get("error_message") or "unknown"),
                str(failed.get("command") or batch[0].get("command", "?")),
                session_id=session_id, command_id=command_id,
                phase="execute", last_status=str(result.get("status") or "failed"),
            )
        for step in steps:
            step["_bro_session_id"] = session_id
            step["_bro_command_id"] = command_id
        return steps


# --------------------------------------------------------------------------- #
# Step payload helpers
# --------------------------------------------------------------------------- #

def fetch_offloaded(url: str, *, timeout: float = 180.0, max_retries: int = 3,
                    session_id: str | None = None, command_id: str | None = None) -> Any:
    """Download a payload getbro moved to object storage.

    The SDK has no helper for ``offloaded_data_url``; the object is public
    storage and is fetched with ``requests`` (the SDK's own HTTP library).
    """
    last: Exception | None = None
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=timeout)
            response.raise_for_status()
            raw = response.content.decode("utf-8", "replace")
        except Exception as exc:  # noqa: BLE001 - transport variety
            last = exc
            if attempt < max_retries:
                time.sleep(1.5 * attempt)
            continue
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return raw  # plain-text payloads are returned as-is
    raise BroPayloadError("could not download completed command payload",
                          session_id=session_id, command_id=command_id,
                          phase="payload_download", last_status="done") from last


def step_data(step: dict[str, Any]) -> Any:
    """Payload of a single command step, downloading offloaded data if needed."""
    if step.get("offloaded_data_url"):
        return fetch_offloaded(step["offloaded_data_url"],
                               session_id=step.get("_bro_session_id"),
                               command_id=step.get("_bro_command_id"))
    return step.get("data")


def last_data(steps: list[dict[str, Any]]) -> Any:
    """Payload of the final step in a batch."""
    return step_data(steps[-1]) if steps else None
