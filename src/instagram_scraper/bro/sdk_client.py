"""The getbro management transport through the official ``bro-api-sdk``.

This is the only concrete client. The public surface is that of
:class:`~instagram_scraper.bro.client.BroClient`, whose recovery loops are
reused unchanged; this class provides the primitives underneath them via
the SDK's own objects:

* ``bro.BroClient`` creates, reads, lists and stops sessions and reads the
  account
* ``bro.BroSession`` submits command batches and reads command status
* ``bro.CommandPayload`` carries each command

Session and command state come back as the SDK exposes them. A session's
``status``, ``error_name`` and ``error_message`` are attributes of the
``bro.BroSession`` object refreshed by ``get_info()``; a command's status is
the ``status`` field of ``get_command()``.

The recovery discipline is the base class's: observe an accepted batch under
its command id. After a lost submit response, reconcile by the tagged batch;
only an idempotent batch may be submitted again, never an ambiguous mutation.

One thing the SDK view makes cheap is telling a *queued* command from a
*running* one. A post-mortem of session ``bc8baf12`` (12 September) showed
a command accepted by the API but never handed to the worker, and the VM
then killed for inactivity; the same shape recurred on ``d8b320f1``
(16 September). getbro reports the delivery bug fixed and the inactivity
limit (300 s) applying only while nothing is queued or running, so a long
``pending`` is not proof of a dead channel. Here it is a *diagnostic*:
past ``dispatch_timeout`` it is logged and counted once, the session's own
status is consulted (a dead VM is reported as such), and otherwise the
same command id keeps being observed within the ordinary recovery budget.
An outcome that never settles ends as ``BroCommandOutcomeUnknown`` and the
caller checkpoints without resubmitting that batch.

SDK behaviour worth knowing:

* ``create_session`` blocks until the browser is ``idle`` and has no upper
  bound of its own; :meth:`SdkBroClient.create_session` therefore runs it
  on a worker thread and, past ``create_timeout``, stops the session it
  can find still booting and raises ``BroSessionError(phase="create")``
* HTTP timeouts are the SDK's fixed ones (15-35 s), not clamped to the run
  budget; the budget still bounds observation and recovery loops
* ``management_requests`` counts SDK calls, and one SDK call may be several
  HTTP requests (its create loop, its 404 retries)
"""

from __future__ import annotations

import logging
import re
import threading
import time
from typing import Any, Callable, TypeVar

import bro as _sdk
import requests as _requests
from bro.exceptions import BroAPIError as _SdkError

from ..errors import BroAuthError, BroHTTPError, BroSessionError, BroSubmitNotAccepted
from .client import (BOOTING_STATUSES, DEAD_STATUSES, LIVE_STATUSES, USER_AGENT, BroClient,
                     batch_tag_of)

log = logging.getLogger(__name__)

T = TypeVar("T")

#: what the SDK raises for a rejected call and what ``requests`` raises for
#: a transport fault; anything else is a bug and propagates as is
_WIRE_FAILURES: tuple[type[BaseException], ...] = (_SdkError, _requests.exceptions.RequestException)

#: how the SDK formats a non-2xx answer: ``API Error (404): detail``
_SDK_STATUS = re.compile(r"^API Error \((\d{3})\): ?(.*)$", re.DOTALL)

SDK_VERSION = str(getattr(_sdk, "__version__", "") or "")


class CreateRegistry:
    """Session creates still in flight, shared by every client of one run.

    The SDK hands a session id back only once the VM is up, so a boot that
    is still in flight has no id yet. While another create of the same run
    is in flight, a booting session in the account's list may be that one,
    and a stranded-boot check must not stop it as its own.
    """

    def __init__(self) -> None:
        self.lock = threading.Lock()
        #: one token per create whose SDK call has not returned yet
        self.in_flight: set[object] = set()

    def others(self, token: object) -> int:
        with self.lock:
            return len(self.in_flight - {token})


class SdkBroClient(BroClient):
    """``BroClient`` whose every wire call is made by ``bro-api-sdk``."""

    #: seconds a submitted command may stay ``pending`` before that is
    #: logged as a slow dispatch; healthy sessions measured 0.09-0.4 s, the
    #: two stalls of the delivery bug 11.8 s and 21.4 s
    DISPATCH_TIMEOUT = 45.0
    #: seconds a session may take to boot before it is abandoned; the same
    #: bound the REST edition applied in ``wait_until_idle``
    CREATE_TIMEOUT = 240.0
    #: tolerance between this clock and the API's ``created_at`` when deciding
    #: whether a listed command was created by a submit whose answer was lost
    SUBMIT_CLOCK_SLACK = 5.0
    #: how the command list is read before a lost submit is declared not
    #: accepted: every snapshot must agree, spaced so a late registration
    #: has time to show (the API's own list is eventually consistent)
    SUBMIT_RECONCILE_SNAPSHOTS = 3
    SUBMIT_RECONCILE_PAUSE = 2.0

    def __init__(
        self,
        api_key: str,
        *,
        base_url: str = "https://api.getbro.ws",
        timeout: float = 120.0,
        dispatch_timeout: float | None = None,
        create_timeout: float | None = None,
        sdk: Any | None = None,
    ) -> None:
        super().__init__(api_key, base_url=base_url, timeout=timeout)
        if sdk is None:
            sdk = _sdk.BroClient(api_key, base_url=base_url, timeout=timeout)
            # api.getbro.ws sits behind a WAF; the SDK's requests agent passes
            # today, but a browser-like one is what the REST edition proved.
            sdk.headers.setdefault("User-Agent", USER_AGENT)
        self.sdk = sdk
        self.dispatch_timeout = (self.DISPATCH_TIMEOUT if dispatch_timeout is None
                                 else float(dispatch_timeout))
        self.create_timeout = (self.CREATE_TIMEOUT if create_timeout is None
                               else float(create_timeout))
        self.stalled_dispatches = 0
        self.abandoned_creates = 0
        #: lost submit responses resolved by the command list: the API had
        #: created the command (adopted) / had not (reported, may be resent)
        self.adopted_submits = 0
        self.rejected_submits = 0
        #: lost submits left unknown because the list showed commands this
        #: client cannot claim (another user of the session, a mismatch)
        self.ambiguous_submits = 0
        #: a first submit that registered late, after its batch was re-sent
        #: (read-only by construction, so the duplicate is harmless)
        self.duplicate_submits = 0
        #: stranded boots left running because the account had more than one
        #: candidate session and none could be proven ours
        self.unclaimed_boots = 0
        self._sessions: dict[str, Any] = {}
        #: session id -> (submit start, first command name, batch tag) of the
        #: last submit declared not accepted, checked again after the re-send
        self._rejected: dict[str, tuple[float, str, str | None]] = {}
        #: session id -> command ids this process submitted or adopted
        self._known: dict[str, set[str]] = {}
        #: command id -> monotonic time of submission, while still queued
        self._queued: dict[str, float] = {}
        #: commands already reported as slow to dispatch (once each)
        self._reported: set[str] = set()
        self._registry_lock = threading.Lock()
        #: creates in flight; a run with several clients gives them one registry
        self.creates = CreateRegistry()

    # ------------------------------------------------------------ plumbing --

    def _call(self, action: Callable[[], T], *, url: str = "") -> T:
        """Run one SDK call, translating its failures into our hierarchy."""
        self.management_requests += 1
        try:
            return action()
        except _WIRE_FAILURES as exc:
            raise _translate(exc, url) from exc

    def _session(self, session_id: str) -> Any:
        """The SDK session object for ``session_id``, fetched once and kept."""
        with self._registry_lock:
            session = self._sessions.get(session_id)
        if session is None:
            session = self._call(lambda: self.sdk.get_session(session_id),
                                 url=f"/v1/sessions/{session_id}")
            with self._registry_lock:
                self._sessions.setdefault(session_id, session)
                session = self._sessions[session_id]
        return session

    # ------------------------------------------------------------- account --

    def get_user(self) -> dict[str, Any]:
        return dict(self._call(self.sdk.get_user, url="/v1/user") or {})

    # ------------------------------------------------------------ sessions --

    def create_session(self, **kwargs: Any) -> str:
        """Create a session through the SDK, bounded by ``create_timeout``.

        The SDK's ``create_session`` returns only once the VM is ``idle``
        and never gives up on its own. It runs here on a daemon thread; if
        the bound (clamped to the run budget) passes first, the session
        still booting under this account is stopped so it cannot keep
        billing, and ``BroSessionError(phase="create")`` is raised. A
        session the thread hands back after that point is stopped too.
        """
        payload = {k: v for k, v in kwargs.items() if v is not None}
        started_at = time.time()
        outcome: dict[str, Any] = {}
        done = threading.Event()
        handoff = threading.Lock()
        creates, token = self.creates, object()
        with creates.lock:
            creates.in_flight.add(token)

        def boot() -> None:
            session: Any = None
            error: BaseException | None = None
            try:
                session = self.sdk.create_session(**payload)
            except BaseException as exc:  # noqa: BLE001 - re-raised on the caller's thread
                error = exc
            finally:
                with creates.lock:
                    creates.in_flight.discard(token)
            with handoff:
                abandoned = bool(outcome.get("abandoned"))
                if not abandoned:
                    outcome["session"], outcome["error"] = session, error
                    done.set()
            if abandoned:
                # The caller gave up already; whatever booted is ours to stop.
                if session is not None:
                    self._stop_quietly(str(getattr(session, "session_id", "") or ""),
                                       reason="booted after the create bound")
                elif error is not None:
                    log.debug("abandoned session create ended with %s", error)

        self.management_requests += 1
        worker = threading.Thread(target=boot, name="getbro-create-session", daemon=True)
        worker.start()
        bound = self._budget(self.create_timeout)
        if not done.wait(bound):
            with handoff:
                gave_up = not done.is_set()
                if gave_up:
                    outcome["abandoned"] = True
            if gave_up:
                self.abandoned_creates += 1
                stranded = self._stop_stranded_session(started_at, token)
                raise BroSessionError(
                    f"session did not become idle within {bound:.0f}s"
                    + (f" (stopped {stranded})" if stranded else ""),
                    session_id=stranded or None, phase="create", last_status="queued")
        if outcome.get("error") is not None:
            error = outcome["error"]
            if isinstance(error, _WIRE_FAILURES):
                translated = _translate(error, "/v1/sessions")
                if (isinstance(translated, BroHTTPError) and translated.status == 0
                        and "failed to initialize" in translated.body.lower()):
                    raise BroSessionError(translated.body, phase="create") from error
                raise translated from error
            raise error
        session = outcome["session"]
        session_id = str(getattr(session, "session_id", "") or "")
        if not session_id:
            raise BroSessionError("the SDK returned a session without an id")
        with self._registry_lock:
            self._sessions[session_id] = session
        return session_id

    def _stop_stranded_session(self, started_at: float, token: object | None = None) -> str:
        """Stop the session this client started that is still booting.

        The SDK never handed the session back, so its id is unknown; the
        account's session list is the only witness. A session is claimed
        only when it is the **single** booting (``queued``/``initializing``)
        session created at or after ``started_at`` (with clock slack), not
        one this client already holds, and no other create of the run
        (``creates``, the browsers of a parallel run) is still in flight.
        Otherwise nothing can be proven ours: none is stopped, they are
        named in the log and counted as ``unclaimed_boots``. A session that
        finishes booting later is stopped by the create thread itself (see
        ``create_session``). Returns the stopped id or an empty string.
        """
        try:
            listed = self._call(lambda: self.sdk.list_sessions(limit=30), url="/v1/sessions")
        except BroHTTPError as exc:
            log.warning("could not list sessions to stop a stranded boot: %s", exc)
            return ""
        with self._registry_lock:
            held = set(self._sessions)
        candidates = []
        for entry in list((listed or {}).get("sessions") or []):
            session_id = str(entry.get("session_id") or "")
            if not session_id or session_id in held:
                continue
            if str(entry.get("status") or "") not in BOOTING_STATUSES:
                continue
            created = _created_at(entry)
            if created is not None and created < started_at - self.SUBMIT_CLOCK_SLACK:
                continue
            candidates.append(session_id)
        siblings = self.creates.others(token)
        if len(candidates) == 1 and not siblings:
            self._stop_quietly(candidates[0], reason="still booting past the create bound")
            return candidates[0]
        if candidates:
            self.unclaimed_boots += 1
            log.warning("%d session(s) booting on this account (%s)%s; none can be proven ours, "
                        "none stopped", len(candidates), ", ".join(c[:8] for c in candidates),
                        f" while {siblings} other create(s) of this run are in flight" if siblings else "")
        return ""

    def _stop_quietly(self, session_id: str, *, reason: str) -> None:
        if not session_id:
            return
        try:
            self._call(lambda: self.sdk.stop_session(session_id), url=f"/v1/sessions/{session_id}")
            log.warning("stopped session %s: %s", session_id[:8], reason)
        except BroHTTPError as exc:
            log.warning("could not stop session %s (%s): %s", session_id[:8], reason, exc)

    def get_session(self, session_id: str) -> dict[str, Any]:
        session = self._session(session_id)
        return dict(self._call(session.get_info, url=f"/v1/sessions/{session_id}") or {})

    def get_session_once(self, session_id: str, *, timeout: float = 15) -> dict[str, Any]:
        return self.get_session(session_id)

    def list_sessions(self, **params: Any) -> list[dict[str, Any]]:
        """The account's sessions, newest first (``limit``/``offset``/``status``)."""
        listed = self._call(lambda: self.sdk.list_sessions(**params), url="/v1/sessions")
        return list((listed or {}).get("sessions") or [])

    def stop_session(self, session_id: str) -> dict[str, Any]:
        return dict(self._call(lambda: self.sdk.stop_session(session_id),
                               url=f"/v1/sessions/{session_id}") or {})

    def wait_until_idle(self, session_id: str, *, timeout: float = 240.0) -> dict[str, Any]:
        """Return the session snapshot once it is usable.

        A freshly created session already is: the SDK's ``create_session``
        only returns on ``idle``. An attached one may still be booting.
        """
        deadline = time.monotonic() + timeout
        session = self._session(session_id)
        info: dict[str, Any] = {}
        while time.monotonic() < deadline:
            info = self.get_session(session_id)
            status = str(session.status or info.get("status") or "")
            if status in LIVE_STATUSES:
                return info
            if status in DEAD_STATUSES:
                raise BroSessionError(
                    f"session {session_id} became {status}: "
                    f"{session.error_name} {session.error_message}",
                    session_id=session_id, phase="create", last_status=status)
            time.sleep(1.5)
        raise BroSessionError(
            f"session {session_id} did not become idle within {timeout:.0f}s "
            f"(last status: {info.get('status')})",
            session_id=session_id, phase="create", last_status=str(info.get("status") or ""))

    # ------------------------------------------------------------ commands --

    def submit(self, session_id: str, commands: list[dict[str, Any]], *,
               idempotent: bool = False, nonce: str | None = None) -> str:
        """Queue a batch as SDK payload models and return its command id.

        The SDK's execute POST has a fixed 15 s timeout. When the response is
        lost, the session's command list (newest first) is read several times
        (``SUBMIT_RECONCILE_SNAPSHOTS``, spaced ``SUBMIT_RECONCILE_PAUSE``)
        to find the command the API may have created for it:

        * a single command this client has never seen, created after the
          submit started, whose first command name matches, is a candidate;
          it is adopted only once its executed steps carry the batch's
          identity (``nonce``, see ``tag_batch``) -- names and timestamps do
          not identify a batch, the tag does;
        * a candidate that executed without the tag belongs to someone else;
          several candidates, or one still pending (nothing to verify), are
          ambiguous; all of these stay unknown;
        * no command in any snapshot: a read-only batch (``idempotent``) is
          reported as :class:`BroSubmitNotAccepted` so the caller may send
          it once more -- safe because it cannot act twice, not because the
          empty snapshots prove anything; any other batch stays unknown.

        A re-sent batch is checked once more after its accepted submit: a
        first command that registered late is counted as a duplicate
        (harmless, the batch was read-only) and left to run.
        """
        session = self._session(session_id)
        payloads = [self.sdk_payload(command) for command in commands]
        first_name = str((commands[0] if commands else {}).get("command") or "")
        started_at = time.time()
        try:
            submitted = self._call(
                lambda: session.execute(payloads, await_completion=False),
                url=f"/v1/sessions/{session_id}/execute")
        except BroHTTPError as exc:
            if exc.status != 0:
                raise
            verdict, command_id = self._reconcile_submit(session, session_id, started_at,
                                                         first_name, nonce, exc)
            if verdict == "adopted":
                return command_id
            if verdict == "not_accepted" and idempotent:
                self._rejected[session_id] = (started_at, first_name, nonce)
                raise BroSubmitNotAccepted(
                    f"submit response lost and no command of ours is listed: {exc.body[:200]}",
                    session_id=session_id, phase="submit", last_status="not_accepted") from exc
            if verdict == "not_accepted":
                log.warning("[%s] submit response lost, no command of ours listed, but the batch (%s) "
                            "is not read-only: not re-sent, outcome stays unknown",
                            session_id[:8], first_name)
            raise exc
        command_id = str((submitted or {}).get("command_id") or "")
        if not command_id:
            raise BroSessionError("the SDK accepted a batch but returned no command id",
                                  session_id=session_id, phase="submit")
        self._remember(session_id, command_id)
        rejected = self._rejected.pop(session_id, None)
        if rejected is not None:
            self._check_late_duplicate(session, session_id, rejected, command_id)
        return command_id

    def _remember(self, session_id: str, command_id: str) -> None:
        with self._registry_lock:
            self._queued[command_id] = time.monotonic()
            self._known.setdefault(session_id, set()).add(command_id)

    def _unknown_commands_since(self, session: Any, session_id: str, started_at: float) -> list[dict[str, Any]]:
        """Commands in the session's list this client never submitted or
        adopted, created at or after ``started_at`` (with clock slack).

        The public list is one row per *step*, newest first, the same
        ``command_id`` repeated with each step's name (measured 18 September:
        a three-step batch listed as ``get_url`` and ``run_js`` rows sharing
        one id), so rows are folded per command id and ``commands`` holds the
        step names seen. Raises ``BroHTTPError`` when the list cannot be read."""
        listed = self._call(lambda: session.get_commands(limit=30),
                            url=f"/v1/sessions/{session_id}/commands")
        with self._registry_lock:
            known = set(self._known.get(session_id, ()))
        found: dict[str, dict[str, Any]] = {}
        for entry in list((listed or {}).get("commands") or []):
            command_id = str(entry.get("command_id") or entry.get("_id") or "")
            if not command_id:
                continue
            if command_id in known:
                break  # newest known command reached: nothing older is new
            created = _created_at(entry)
            if created is not None and created < started_at - self.SUBMIT_CLOCK_SLACK:
                break  # predates the submit: an earlier process's command
            item = found.setdefault(command_id, {"command_id": command_id, "commands": [],
                                                 "created_at": created, "status": entry.get("status")})
            name = str(entry.get("command") or "")
            if name and name not in item["commands"]:
                item["commands"].append(name)
        return list(found.values())

    def _reconcile_submit(self, session: Any, session_id: str, started_at: float,
                          first_name: str, nonce: str | None, fault: BroHTTPError) -> tuple[str, str | None]:
        """After a lost submit response, decide from repeated command-list
        snapshots: ``("adopted", id)``, ``("not_accepted", None)`` or
        ``("ambiguous", None)``. Re-raises the fault when the list is unreadable."""
        for snapshot in range(self.SUBMIT_RECONCILE_SNAPSHOTS):
            if snapshot:
                time.sleep(self.SUBMIT_RECONCILE_PAUSE)
            try:
                found = self._unknown_commands_since(session, session_id, started_at)
            except BroHTTPError as exc:
                log.warning("[%s] submit response lost and the command list is unreadable (%s); "
                            "outcome stays unknown", session_id[:8], exc)
                raise fault from exc
            if not found:
                continue  # nothing yet: give a late registration time to show
            if len(found) == 1 and (not found[0]["commands"] or first_name in found[0]["commands"]):
                command_id = found[0]["command_id"]
                verdict = self._verify_candidate(session, session_id, command_id, nonce)
                if verdict == "ours":
                    self.adopted_submits += 1
                    self._remember(session_id, command_id)
                    log.warning("[%s] submit response lost; adopted command %s (%s) -- its steps "
                                "carry this batch's tag", session_id[:8], command_id[:8], first_name)
                    return "adopted", command_id
                reason = ("executed without this batch's tag" if verdict == "foreign"
                          else "still pending, nothing to verify")
            else:
                reason = ", ".join(f"{f['command_id'][:8]}:{'/'.join(f['commands']) or '?'}" for f in found)
            self.ambiguous_submits += 1
            log.warning("[%s] submit response lost and the command list is ambiguous (%s); "
                        "outcome stays unknown", session_id[:8], reason)
            return "ambiguous", None
        self.rejected_submits += 1
        log.warning("[%s] submit response lost and %d snapshots list no command of ours",
                    session_id[:8], self.SUBMIT_RECONCILE_SNAPSHOTS)
        return "not_accepted", None

    def _verify_candidate(self, session: Any, session_id: str, command_id: str,
                          nonce: str | None) -> str:
        """``"ours"`` when the candidate's executed steps end with this batch's
        tag, ``"foreign"`` when it executed without it, ``"unverified"`` when it
        has not executed yet or cannot be read."""
        if not nonce:
            return "unverified"
        try:
            data = dict(self._call(lambda: session.get_command(command_id),
                                   url=f"/v1/sessions/{session_id}/commands/{command_id}") or {})
        except BroHTTPError:
            return "unverified"
        steps = list((data.get("response") or {}).get("commands") or [])
        if not steps:
            return "unverified"
        tag = batch_tag_of(steps)
        if tag == nonce:
            return "ours"
        if tag is None and str(data.get("status") or "") not in ("done", "failed", "stopped", "cancelled"):
            return "unverified"   # still running: the tag step has not been reached
        return "foreign"

    def _check_late_duplicate(self, session: Any, session_id: str,
                              rejected: tuple[float, str, str | None], resent_id: str) -> None:
        """After a re-send: did the first submit register after all? A late
        command is claimed as our duplicate only when its steps carry the
        batch's tag (or it has not executed yet and the name matches);
        anything else is someone else's and is left alone."""
        started_at, first_name, nonce = rejected
        try:
            found = self._unknown_commands_since(session, session_id, started_at)
        except BroHTTPError as exc:
            log.warning("[%s] could not check for a late duplicate of the re-sent batch: %s",
                        session_id[:8], exc)
            return
        late = []
        for entry in found:
            if entry["command_id"] == resent_id:
                continue
            if entry["commands"] and first_name not in entry["commands"]:
                continue
            if self._verify_candidate(session, session_id, entry["command_id"], nonce) != "foreign":
                late.append(entry)
        if late:
            self.duplicate_submits += len(late)
            with self._registry_lock:
                self._known.setdefault(session_id, set()).update(f["command_id"] for f in late)
            log.warning("[%s] the lost submit (%s) registered late after its re-send: %s -- a "
                        "read-only duplicate, left to run", session_id[:8], first_name,
                        ", ".join(f["command_id"][:8] for f in late))

    @staticmethod
    def sdk_payload(command: dict[str, Any]) -> Any:
        """One of our command dicts as the SDK's ``CommandPayload`` model."""
        return _sdk.CommandPayload(command=str(command["command"]), params=command.get("params"))

    def get_command(self, session_id: str, command_id: str, *, timeout: float = 30) -> dict[str, Any]:
        session = self._session(session_id)
        data = dict(self._call(lambda: session.get_command(command_id),
                               url=f"/v1/sessions/{session_id}/commands/{command_id}") or {})
        self._watch_dispatch(session, session_id, command_id, str(data.get("status") or ""))
        return data

    def _watch_dispatch(self, session: Any, session_id: str, command_id: str, status: str) -> None:
        """Diagnose, once, a command the worker is slow to pick up.

        ``pending`` means queued at the API; ``running`` means the worker has
        it. A long HAR dump is ``running`` for minutes and is left alone. A
        command still ``pending`` after ``dispatch_timeout`` is logged and
        counted, and the session's own status is read: a VM that is already
        dead is reported as such. A live session proves nothing about the
        command either way, so observation of the same command id simply
        continues -- the caller's timeout and recovery budget bound it, and
        an outcome that never settles surfaces as unknown, never as a
        re-submission.
        """
        with self._registry_lock:
            if status != "pending":
                self._queued.pop(command_id, None)
                self._reported.discard(command_id)
                return
            if command_id in self._reported:
                return
            queued_at = self._queued.get(command_id)
        if queued_at is None:
            # First sight of a command this process did not submit (a resume
            # re-reading a checkpointed id): start the clock now.
            with self._registry_lock:
                self._queued[command_id] = time.monotonic()
            return
        waited = time.monotonic() - queued_at
        if waited < self.dispatch_timeout:
            return
        with self._registry_lock:
            # Reported once per command; later reads keep observing quietly.
            self._queued.pop(command_id, None)
            self._reported.add(command_id)
        self.stalled_dispatches += 1
        try:
            self._call(session.get_info, url=f"/v1/sessions/{session_id}")
        except BroHTTPError:
            pass  # the SDK object keeps its last known state
        session_status = str(session.status or "")
        if session_status in DEAD_STATUSES:
            raise BroSessionError(
                f"browser session is {session_status}: {session.error_name}: "
                f"{session.error_message}",
                session_id=session_id, command_id=command_id, phase="dispatch",
                last_status="pending")
        log.warning("[%s] command %s still pending after %.0fs while the session is %s; "
                    "continuing to observe the same command id", session_id[:8],
                    command_id[:8], waited, session_status or "unknown")


def _created_at(entry: dict[str, Any]) -> float | None:
    """A session's ``created_at`` (ISO 8601, UTC) as a Unix timestamp."""
    raw = entry.get("created_at")
    if not raw:
        return None
    if isinstance(raw, (int, float)):
        return float(raw)
    from datetime import datetime, timezone
    text = str(raw).strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.timestamp()


def _translate(exc: Exception, url: str) -> Exception:
    """Map an SDK or transport exception onto the scraper's hierarchy."""
    if isinstance(exc, _SdkError):
        match = _SDK_STATUS.match(str(exc))
        if not match:
            return BroHTTPError(0, str(exc), url)
        status, detail = int(match.group(1)), match.group(2)
        if status in (401, 403) and "getbro" not in detail.lower():
            return BroAuthError(f"getbro rejected the API key (HTTP {status}): {detail[:300]}")
        if status == 402:
            return BroAuthError(f"getbro balance exhausted: {detail[:300]}")
        return BroHTTPError(status, detail, url)
    return BroHTTPError(0, f"{type(exc).__name__}: {exc}", url)
