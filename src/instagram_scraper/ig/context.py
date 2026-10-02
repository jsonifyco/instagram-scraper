"""Bootstrapping an Instagram browsing context inside a getbro session.

Responsibilities:

* warm the session up on instagram.com so it holds real cookies
* inject user-supplied `sessionid` cookies and report whether login stuck
* read the live ``lsd`` / ``csrftoken`` / ``X-IG-App-ID`` values off the page,
  which the GraphQL calls need
* run `fetch_json` calls and classify the answer (JSON / login wall / rate
  limit / not found), so callers get an exception instead of an HTML string
"""

from __future__ import annotations

import json
import hashlib
import logging
import random
import time
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlsplit

from ..errors import (
    ChallengeRequiredError,
    EndpointNotServedError,
    InstagramError,
    InstagramServerError,
    PageFetchUnresponsiveError,
    PageFetchReadTimeoutError,
    LoginRequiredError,
    NotFoundError,
    RateLimitedError,
    FatalError,
    BroCommandError,
    BroCommandOutcomeUnknown,
    BroPayloadError,
    BroSessionError,
)
from ..bro import commands as cmd
from ..bro.session import BroSession
from ..bro.client import LIVE_STATUSES
from . import endpoints as ep

log = logging.getLogger(__name__)

#: Pages where Instagram wants the account owner to act before the session
#: can be used: a checkpoint, a password step, or the security check
#: ("Confirm it's you") under ``/auth_platform/`` -- measured 2026-09-29, a
#: challenged account landed on ``/auth_platform/challengepicker/`` with no
#: login form and its viewer cookie in place, and passed as logged in.
RESTRICTED_PATHS = ("/challenge/", "/checkpoint/", "/accounts/password/", "/auth_platform/")

#: JS that harvests the tokens Instagram embeds in its bootstrap payloads.
_TOKEN_JS = """
(() => {
  const html = document.documentElement.outerHTML;
  const pick = (re) => { const m = html.match(re); return m ? m[1] : null; };
  const cookie = (name) => {
    const m = document.cookie.match(new RegExp('(?:^|; )' + name + '=([^;]*)'));
    return m ? decodeURIComponent(m[1]) : null;
  };
  return JSON.stringify({
    lsd: pick(/"LSD",\\[\\],\\{"token":"([^"]+)"/) || pick(/"lsd":"([^"]+)"/),
    appId: pick(/"X-IG-App-ID":"(\\d+)"/) || pick(/"APP_ID":"(\\d+)"/),
    rev: pick(/"__spin_r":(\\d+)/),
    csrf: cookie('csrftoken'),
    userId: cookie('ds_user_id'),
    // Instagram no longer emits "is_logged_in":true, so a live session is
    // inferred from the absence of the logged-out shell instead. Keep the
    // old marker as a positive hint for renders that still carry it.
    loggedInMarker: html.indexOf('"is_logged_in":true') !== -1,
    loginForm: !!document.querySelector('input[name="username"]')
            || !!document.querySelector('input[type="password"]')
            || !!document.querySelector('form#loginForm'),
    // The logged-out landing -- including the saved-account "Continue as
    // <user>" screen a dead session lands on -- offers sign-up; the logged-in
    // web app never does, and always links Direct in its navigation.
    signupLink: !!document.querySelector('a[href*="/accounts/emailsignup/"]'),
    directLink: !!document.querySelector('a[href^="/direct/"]'),
    loginPrompt: (document.body.innerText || '').indexOf('Log into Instagram') !== -1,
    loggedOutClass: html.indexOf('not-logged-in') !== -1,
    restricted: __RESTRICTED__.some(p => location.pathname.includes(p)),
    path: location.pathname,
    pageLoaded: !!document.querySelector('script[src*="cdninstagram.com"], script[data-sjs]'),
    proxyError: /Upstream proxy|ERR_PROXY_CONNECTION_FAILED|ERR_TUNNEL_CONNECTION_FAILED/.test(document.body.innerText || ''),
    title: document.title
  });
})()
""".replace("__RESTRICTED__", json.dumps(list(RESTRICTED_PATHS)))

#: Finds "Continue" on the saved-account screen and returns the centre of
#: the button for a trusted pointer click (the page ignores a synthetic
#: ``element.click()``: measured 2026-09-24, no request at all). Only a button
#: whose whole label is "Continue" -- in the languages this route has served
#: -- is ever pressed; nothing is typed, no other control is touched.
_CONTINUE_JS = r"""
(() => {
  // savedLoginContinue
  const labels = /^(continue|weiter|продолжить|continuer|continuar|continua)$/i;
  const buttons = Array.from(document.querySelectorAll('button, [role="button"]'))
    .filter(e => e.offsetParent !== null && (e.innerText || '').trim());
  const target = buttons.find(e => labels.test((e.innerText || '').trim()));
  if (!target) {
    return JSON.stringify({found: false,
      buttons: buttons.map(e => (e.innerText || '').trim().slice(0, 30)).slice(0, 6)});
  }
  const r = target.getBoundingClientRect();
  return JSON.stringify({found: true, label: (target.innerText || '').trim().slice(0, 30),
    x: Math.round(r.x + r.width / 2), y: Math.round(r.y + r.height / 2)});
})()
"""

_LOGIN_MARKERS = (
    "loginform", "login_required", "please wait a few minutes",
    '"require_login"', "accounts/login", "not-logged-in",
)
_CHALLENGE_MARKERS = ("challenge_required", "checkpoint_required", "/challenge/")
_RATE_MARKERS = ("please wait a few minutes before you try again",
                 "rate limited", "too many requests")

#: REST reads the logged-in web client no longer makes (``feed/user`` and
#: ``clips/user`` answered its HTML app page on 2026-09-24; ``usertags``,
#: ``users/{id}/info`` and ``web_profile_info`` 429 without JSON on
#: 2026-09-29) live in ``endpoints.WEB_RETIRED_PATHS``; ``_unwrap`` reads such
#: an answer as :class:`EndpointNotServedError`.



@dataclass(slots=True)
class IgTokens:
    """Live values harvested from an Instagram page."""

    lsd: str | None = None
    app_id: str = ep.WEB_APP_ID
    csrf: str | None = None
    rev: str | None = None
    user_id: str | None = None
    logged_in: bool = False
    #: the page showed the logged-in navigation (a Direct link), not only
    #: the viewer cookie
    web_confirmed: bool = False
    #: why a page with the viewer cookie still counts as logged out
    logged_out_reason: str | None = None
    #: the page asks for a password (the scraper never enters one)
    password_prompt: bool = False
    #: the path the page was on when these were read
    page_path: str | None = None


#: seconds a page-context GET may stay in flight before its outcome is
#: declared unknown (the slot is kept; a resume reads it, never re-fetches)
PAGE_FETCH_WAIT = 30.0
PAGE_FETCH_FAST_WAIT_MS = 8000
PAGE_FETCH_LOCATOR_WAIT_MS = 12000
#: invocations in which the same page-context read may end unanswered
#: (unknown or deferred) before the task is classified rather than retried
PAGE_FETCH_UNANSWERED_LIMIT = 2

#: Submit one same-origin GET from the page's own JavaScript, keyed so that
#: a repeated pending/decoded submit with the same key finds the existing
#: slot. Keys whose pages were *applied* in SQLite may be cleared by the next
#: submit, in that same command; no extra getbro command or unjournaled ack.
_PAGE_FETCH_JS = """(function(u,k,applied,o){
  window.__igsReads = window.__igsReads || {};
  function markSettled(){
    if(document.documentElement) document.documentElement.setAttribute('data-igs-read-settled',k);
  }
  applied.forEach(function(old){
    var slot=window.__igsReads[old];
    if (old!==k && slot && slot.state==='done') delete window.__igsReads[old];
  });
  if (document.documentElement) document.documentElement.removeAttribute('data-igs-read-settled');
  if (window.__igsReads[k]) {
    if (window.__igsReads[k].state !== 'pending') markSettled();
    return 'exists';
  }
  window.__igsReads[k] = {state:'pending', started: Date.now()};
  // A late response still uses the existing slot. Waking the locator before
  // its deadline lets the batch finish with its identity tag intact.
  setTimeout(function(){
    if(window.__igsReads[k] && window.__igsReads[k].state==='pending') markSettled();
  }, __FAST_WAIT__);
  var init = {credentials:'include'};
  if (o) {
    init.method = o.method || 'GET';
    if (o.body !== null && o.body !== undefined) init.body = o.body;
    var h = {};
    if (o.contentType) h['content-type'] = o.contentType;
    if (o.web) {
      var m = document.cookie.match(/(?:^|; )csrftoken=([^;]+)/);
      h['x-ig-app-id'] = o.web.appId; h['x-asbd-id'] = o.web.asbd;
      h['x-requested-with'] = 'XMLHttpRequest';
      if (m) h['x-csrftoken'] = decodeURIComponent(m[1]);
      h['x-ig-www-claim'] = sessionStorage.getItem('www-claim-v2') || '0';
    }
    init.headers = h;
  }
  var controller=null, timer=null, cancelled=false, responseStatus=0;
  if ((!init.method || init.method==='GET') && o && o.timeoutMs && typeof AbortController!=='undefined') {
    controller=new AbortController(); init.signal=controller.signal;
    timer=setTimeout(function(){ cancelled=true; controller.abort(); }, o.timeoutMs);
  }
  fetch(u, init).then(async function(r){
    responseStatus=r.status;
    var started=window.__igsReads[k].started;
    window.__igsReads[k] = {state:'done', status:r.status, body: await r.text(), started:started, finished: Date.now()};
    markSettled();
  }).catch(function(e){
    window.__igsReads[k] = {state:'error', error:String(e), status:responseStatus,
      aborted:cancelled && e && e.name==='AbortError', finished: Date.now()};
    markSettled();
  }).finally(function(){ if(timer!==null) clearTimeout(timer); });
  return 'submitted';
})(__URL__, __KEY__, __APPLIED__, __OPTS__)"""

_PAGE_READ_JS = """(function(k){var s=(window.__igsReads||{})[k]; return JSON.stringify(s||null);})(__KEY__)"""


def page_fetch_key(fingerprint: str, attempt: int = 1) -> str:
    """The page-side identity of a managed read: derived from the request
    fingerprint the operation journal already stores, so a resume can find
    the slot without any extra state."""
    # A confirmed server failure may be retried. Each retry needs a new slot,
    # while recovery of the same attempt must always find the existing slot.
    base = "igs:" + str(fingerprint)[:24]
    return base if int(attempt or 1) <= 1 else f"{base}:{int(attempt)}"


def page_fetch_payload(slot: dict[str, Any] | None) -> dict[str, Any] | None:
    """A page-fetch slot in the shape ``fetch_json`` returns, or None while
    the request is still pending."""
    if not isinstance(slot, dict):
        return None
    state = slot.get("state")
    if state == "done":
        text = str(slot.get("body") or "")
        try:
            decoded = json.loads(text) if text else None
        except ValueError:
            decoded = None
        return {"result": {"status": int(slot.get("status") or 0), "json": decoded, "text": text,
                           "transport": "page_fetch"}}
    if state == "error":
        return {"result": {"status": int(slot.get("status") or 0), "json": None,
                           "pageFetchAborted": slot.get("aborted") is True,
                           "text": str(slot.get("error") or "fetch failed"),
                           "transport": "page_fetch"}}
    return None


def read_page_fetch_slot(session, key: str, *, operation_store=None,
                         parent_operation=None, flush=None) -> dict[str, Any] | None:
    """Read a slot without fetching again. Journal the observation command
    separately: losing its result must never replace the fetch-submit ID."""
    script = _PAGE_READ_JS.replace("__KEY__", json.dumps(key))
    poll_id = None
    if operation_store is not None and parent_operation:
        poll_id = operation_store.begin_operation(
            parent_operation["job_id"], "page_fetch_poll",
            parent_operation["request_fingerprint"], parent_operation.get("attempt", 1))

    def event(state, fields):
        if poll_id:
            operation_store.update_operation(
                poll_id, state, session_id=session.session_id,
                command_id=fields.get("command_id"), phase=fields.get("phase"),
                last_status=fields.get("last_status"))
            if state in ("recovering", "observation_lost", "outcome_unknown") and flush:
                flush()

    try:
        if poll_id:
            # Reading a slot changes nothing: safe to send again if its
            # submit response is lost.
            payload = session.run_one(cmd.run_js(script, out_type="str"), retries=0, on_event=event,
                                      idempotent=True)
            raw = (payload or {}).get("result")
        else:
            raw = session.js(script, out_type="str")
    except (BroCommandError, BroCommandOutcomeUnknown, BroPayloadError, BroSessionError) as exc:
        if poll_id:
            state = "outcome_unknown" if isinstance(exc, BroCommandOutcomeUnknown) else (
                "payload_error" if isinstance(exc, BroPayloadError) else "failed")
            operation_store.update_operation(
                poll_id, state, session_id=getattr(exc, "session_id", None),
                command_id=getattr(exc, "command_id", None), phase=getattr(exc, "phase", None),
                last_status=getattr(exc, "last_status", None), error_type=type(exc).__name__)
            # _send must keep the submit operation intact; the unresolved
            # observation command has to be resolved first on resume.
            exc.page_fetch_poll_failure = True
        raise
    try:
        slot = json.loads(raw) if raw else None
    except ValueError:
        slot = None
    if poll_id:
        operation_store.update_operation(poll_id, "applied", phase="page_fetch_observation", last_status="done")
    return slot if isinstance(slot, dict) else None


def wait_page_fetch_slot(session, key: str, *, operation_store=None,
                         parent_operation=None, flush=None, time_remaining=None,
                         initial_slot=None, missing_ok=False,
                         wait: float | None = None) -> dict[str, Any] | None:
    """Bounded read-only observation of a page fetch. A pending slot is never
    downgraded to 'missing': it stops the run with its outcome still unknown.
    ``wait`` stretches the bound to the run's read timeout when that is longer."""
    deadline = time.monotonic() + max(PAGE_FETCH_WAIT, wait or 0)
    slot = initial_slot
    while True:
        if slot is None:
            available = time_remaining() if time_remaining else None
            if available is not None and available <= 0:
                break
            slot = read_page_fetch_slot(
                session, key, operation_store=operation_store,
                parent_operation=parent_operation, flush=flush)
        payload = page_fetch_payload(slot)
        if payload is not None:
            return payload
        if slot is None and missing_ok:
            return None
        remaining = deadline - time.monotonic()
        if time_remaining:
            available = time_remaining()
            if available is not None:
                remaining = min(remaining, available)
        if remaining <= 0:
            break
        time.sleep(min(2, remaining))
        slot = None
    if flush:
        flush()
    raise BroCommandOutcomeUnknown(
        f"page fetch remains pending; slot {key} kept for recovery",
        session_id=session.session_id,
        command_id=(parent_operation or {}).get("command_id"),
        phase="page_fetch", last_status="pending")


@dataclass
class IgContext:
    """An Instagram-aware view over a getbro browser session."""

    session: BroSession
    cookies: list[dict[str, Any]] = field(default_factory=list)
    tokens: IgTokens = field(default_factory=IgTokens)
    bootstrapped: bool = False
    #: URL the browser is currently parked on (drives the Referer header)
    current_page: str = ep.BASE + "/"
    #: VM swaps this context may make on its own when its exit IP is walled
    #: off (see :meth:`_swap_vm_after`). A run's contexts share the run's
    #: allowance through ``budget.take_ip_rotation`` instead.
    max_ip_rotations: int = 0
    #: VM swaps this context made
    rotations: int = 0
    request_pause: float = 3.0
    api_calls: int = 0
    _last_request: float = 0.0
    budget: Any = None
    read_retries: int = 0
    strict_auth: bool = False
    account_bound: bool = False
    authenticated_since: float | None = None
    traffic: Any = None
    operation_store: Any = None
    operation_job_id: str | None = None
    operation_kind: str | None = None
    operation_flush: Any = None
    native_forms: dict[str, dict[str, str]] = field(default_factory=dict)
    _cleaned_page_fetch_keys: set[str] = field(default_factory=set)
    #: seconds getbro lets one read run (fetch_json ``timeout``); None = 30
    read_timeout: float | None = None
    #: press "Continue" once when the cookies land on the saved-account screen
    resume_saved_login: bool = True
    #: what that one press did (reported in the run's preflight)
    saved_login: dict[str, Any] | None = None

    # ------------------------------------------------------------ bootstrap --

    def bootstrap(self, *, landing: str | None = None, force: bool = False) -> IgTokens:
        """Load Instagram once, inject cookies and harvest tokens."""
        if self.bootstrapped and not force:
            return self.tokens
        if self.budget:
            self.budget.check()

        target = landing or f"{ep.BASE}/"
        if self.cookies:
            # The runner permits this only after its anonymous route check.
            # An attached browser uses its existing jar and never comes here.
            self.session.inject_cookies(self.cookies)

        log.debug("bootstrapping Instagram context on %s", target)
        self.session.run([cmd.open_url(target), cmd.sleep(2.5)])
        
        # Close optional cookies popup if it appears
        self.session.run(
            [cmd.click(locator={"strategy": "text", "value": "Decline optional cookies", "timeout_ms": 2000})],
            raise_on_failure=False,
            retries=0
        )
        
        self.current_page = target
        self.tokens = self.read_tokens()
        # The viewer cookie is there at once; whether the page is the
        # logged-in app or the saved-account "Continue" screen shows only
        # once the SPA has rendered. Give it two more short looks (page reads
        # only, no Instagram request) before trusting the cookie alone.
        for _ in range(2 if self.cookies else 0):
            if not self.tokens.logged_in or self.tokens.web_confirmed:
                break
            time.sleep(2.5)
            self.tokens = self.read_tokens()
        if (self.cookies and self.resume_saved_login and not self.tokens.logged_in
                and self.tokens.logged_out_reason == "logged_out_landing"):
            self._continue_saved_login()
        self.bootstrapped = True

        if self.cookies and not self.tokens.logged_in:
            log.warning(
                "session cookies were supplied but Instagram served the "
                "logged-out page%s%s -- the sessionid has expired or been "
                "invalidated. Session-only results (comments, stories, "
                "mentions, account-dependent profile media) will not be available "
                "until you supply a fresh one.",
                " (the saved-account 'Continue' screen)"
                if self.tokens.logged_out_reason == "logged_out_landing" else "",
                f" (ds_user_id={self.tokens.user_id} is still in the jar, "
                "which does not mean the login is live)"
                if self.tokens.user_id else "",
            )
        elif self.tokens.logged_in:
            log.info("Instagram session is authenticated (user id %s)",
                     self.tokens.user_id or "?")
        return self.tokens

    def _continue_saved_login(self) -> None:
        """Press "Continue" once on the saved-account screen, then re-check.

        Cookies whose account Instagram still recognises can land every page
        on "Continue / Use another profile". Where the browser holds a one-tap
        login, that press resumes the session; otherwise Instagram opens its
        password dialog -- measured 2026-09-24 for cookies exported on
        9 September. One trusted press, then the login is judged the usual
        way; a password field, a checkpoint or anything else leaves the
        context logged out and the run stops. The scraper never types a
        password or presses anything else.
        """
        raw = self.session.js(_CONTINUE_JS, out_type="str")
        try:
            facts = json.loads(raw) if isinstance(raw, str) else (raw or {})
        except json.JSONDecodeError:
            facts = {}
        self.saved_login = {"clicked": False, "label": facts.get("label"), "buttons": facts.get("buttons")}
        if not facts.get("found") or facts.get("x") is None or facts.get("y") is None:
            log.warning("saved-account screen without a 'Continue' button (%s); not logged in",
                        facts.get("buttons"))
            return
        self.session.run_one(cmd.click_at(facts["x"], facts["y"]), retries=0)
        self.saved_login["clicked"] = True
        log.info("pressed 'Continue' on the saved-account screen; re-checking the login")
        for wait in (6.0, 3.0, 3.0):
            time.sleep(wait)
            self.tokens = self.read_tokens()
            if self.tokens.web_confirmed or self.tokens.password_prompt:
                break
        self.saved_login.update(loggedIn=self.tokens.logged_in,
                                webConfirmed=self.tokens.web_confirmed,
                                passwordPrompt=self.tokens.password_prompt,
                                reason=self.tokens.logged_out_reason)
        if self.tokens.password_prompt:
            log.warning("Instagram asked for the password after 'Continue'; the scraper "
                        "never enters one")

    def read_tokens(self) -> IgTokens:
        """Re-read tokens from whatever page is loaded."""
        raw = self.session.js(_TOKEN_JS, out_type="str")
        try:
            data = json.loads(raw) if isinstance(raw, str) else (raw or {})
        except json.JSONDecodeError:
            log.debug("token harvest returned non-JSON: %.200s", raw)
            return self.tokens

        if data.get("proxyError"):
            raise FatalError("Browser received a proxy error page; Instagram login was not verified")
        if (self.cookies or self.account_bound) and data.get("restricted"):
            raise FatalError(f"Instagram requires a checkpoint or password action for this account "
                             f"({data.get('path') or 'security page'}); the owner has to confirm it "
                             "in a browser. Stopping before any request")

        return IgTokens(
            lsd=data.get("lsd"),
            app_id=str(data.get("appId") or ep.WEB_APP_ID),
            csrf=data.get("csrf"),
            rev=str(data["rev"]) if data.get("rev") else None,
            user_id=data.get("userId"),
            logged_in=_looks_logged_in(data),
            web_confirmed=bool(data.get("directLink")),
            logged_out_reason=_logged_out_reason(data),
            password_prompt=bool(data.get("loginForm")),
            page_path=data.get("path"),
        )

    @property
    def authenticated(self) -> bool:
        return bool(self.tokens.logged_in)

    # ------------------------------------------------------------ navigation --

    def goto(self, url: str, *, wait: float = 2.5, refresh_tokens: bool = False) -> str:
        """Navigate the browser and remember the page for Referer purposes."""
        if self.budget:
            self.budget.check()
        landed = self.session.goto(url, wait=wait)
        self.current_page = landed or url
        
        # Dismiss the "See photos, videos and more from..." popup if it appears
        self.session.run([
            cmd.locate(strategy="text", value="See photos, videos and more", timeout_ms=2000),
            cmd.press("Escape")
        ], raise_on_failure=False, retries=0)
        
        if (self.cookies or self.account_bound) and any(p in self.current_page for p in RESTRICTED_PATHS):
            raise FatalError("Instagram requires a checkpoint or password action for this account "
                             f"({urlsplit(self.current_page).path}); stopping")
        if refresh_tokens:
            self.tokens = self.read_tokens()
        return self.current_page

    # ------------------------------------------------------------- requests --

    def _send(
        self,
        url: str,
        *,
        method: str = "GET",
        referer: str | None = None,
        body: str | None = None,
        content_type: str | None = None,
        what: str = "this resource",
        allow_html: bool = False,
        transport: str = "fetch_json",
        page_headers: bool = False,
    ) -> Any:
        """One endpoint read. When Instagram walls off an anonymous
        browser's exit IP, the browser moves to a fresh VM and the read is
        repeated there (:meth:`_swap_vm_after`)."""
        while True:
            try:
                return self._send_once(url, method=method, referer=referer, body=body,
                                       content_type=content_type, what=what,
                                       allow_html=allow_html, transport=transport,
                                       page_headers=page_headers)
            except (LoginRequiredError, RateLimitedError) as exc:
                if not self._swap_vm_after(exc, url, what):
                    raise

    def _swap_vm_after(self, exc: InstagramError, url: str, what: str) -> bool:
        """Move this anonymous browser to a fresh VM after ``exc`` and say
        whether the read may be repeated there.

        Only a wall on the exit IP qualifies: a rate limit, or a login wall on
        a read Instagram serves to logged-out visitors
        (:func:`endpoints.anonymous_read`). A read that needs an account
        answers the same on any IP and is not repeated. A browser with cookies
        never moves: one account hopping between IPs is what account-takeover
        detection looks for. Swaps come out of the run's ``maxIpRotations``.
        """
        if self.cookies or self.account_bound or getattr(self.session, "no_restart", False):
            return False
        if isinstance(exc, LoginRequiredError) and not ep.anonymous_read(url):
            return False
        take = getattr(self.budget, "take_ip_rotation", None)
        allowed = take() if callable(take) else self.rotations < self.max_ip_rotations
        if not allowed:
            log.info("%s: %s on this exit IP and no session swap left", what, type(exc).__name__)
            return False
        log.warning("%s: %s on this exit IP; moving the browser to a new bro session",
                    what, type(exc).__name__)
        self.rotate_ip()
        return True

    def _send_once(
        self,
        url: str,
        *,
        method: str = "GET",
        referer: str | None = None,
        body: str | None = None,
        content_type: str | None = None,
        what: str = "this resource",
        allow_html: bool = False,
        transport: str = "fetch_json",
        page_headers: bool = False,
    ) -> Any:
        """One endpoint read on the current VM.

        ``transport`` is getbro's ``fetch_json`` (default) or ``page``: the
        same GET issued by the page's own ``fetch`` with its cookies. The
        reply-thread head continuation needs the latter -- measured on
        16 September, ``fetch_json`` replayed the first four rows for every
        head cursor while the page's fetch of the same URL paged on
        (5, 8, 7, 8, 7, 9 new replies). Everything around the request --
        pacing, budget, journal, rejection checks, pending-page save -- is
        shared; only the execution differs.
        """
        extra = {"content-type": content_type} if content_type else None
        headers = ep.json_headers(
            app_id=self.tokens.app_id,
            referer=referer or self.current_page,
            csrf=self.tokens.csrf,
            extra=extra,
        )
        fingerprint = hashlib.sha256(json.dumps(
            {"method": method, "url": url, "body": body},
            sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        prior_failures = 0
        prior_attempt = 0
        if (transport == "page" and method == "GET" and
                self.operation_store and self.operation_job_id):
            prior_failures, prior_attempt = self.operation_store.db.execute(
                "SELECT count(DISTINCT CASE WHEN state='failed' THEN attempt END), "
                "coalesce(max(CASE WHEN state='failed' THEN attempt END),0) FROM command_operations "
                "WHERE request_fingerprint=? AND operation_kind='page_fetch' AND session_id=?",
                (fingerprint, self.session.session_id)).fetchone()
        allowed_reads = max(0, 1 + (self.read_retries if method == "GET" else 0) - prior_failures)
        if not allowed_reads:
            raise PageFetchUnresponsiveError("confirmed read attempts exhausted for this cursor")
        # A known cancelled GET may use the remaining configured retry
        # later in the same VM. Unknown outcomes and POSTs are not retried.
        for read_attempt in range(allowed_reads):
            self._pace_request(retry=read_attempt > 0 or prior_failures > 0)
            if self.traffic:
                self.traffic.managed(url, method, body)
            operation_id = None
            page_attempt = (prior_attempt if transport == "page" and method == "GET" else 0) + read_attempt + 1
            if self.operation_store and self.operation_job_id:
                operation_id = self.operation_store.begin_operation(
                    self.operation_job_id,
                    "page_fetch" if transport == "page" else (self.operation_kind or what),
                    fingerprint, page_attempt)

            def command_event(state, fields):
                if not operation_id:
                    return
                command_id = fields.get("command_id")
                mapped = "recovered" if state == "recovered" else state
                self.operation_store.update_operation(
                    operation_id, mapped, session_id=self.session.session_id,
                    command_id=command_id, phase=fields.get("phase"),
                    last_status=fields.get("last_status"))
                if state == "recovered":
                    count = self.operation_store.get_meta("recoveredCommands", 0)
                    self.operation_store.set_meta("recoveredCommands", count + 1)
                    invocation_count = self.operation_store.get_meta(
                        "recoveredCommandsThisInvocation", 0)
                    self.operation_store.set_meta(
                        "recoveredCommandsThisInvocation", invocation_count + 1)
                if state in ("recovering", "observation_lost") and self.operation_store.get_meta(
                        "recordsAtFirstCommandFailure") is None:
                    self.operation_store.set_meta(
                        "recordsAtFirstCommandFailure", self.operation_store.count())
                if state in ("recovering", "observation_lost", "outcome_unknown") and self.operation_flush:
                    self.operation_flush()

            try:
                if transport == "page":
                    payload = self._page_fetch(url, command_event, operation_id,
                                               attempt=page_attempt, method=method,
                                               body=body, content_type=content_type,
                                               web_headers=page_headers)
                else:
                    payload = self.session.run_one(
                        cmd.fetch_json(url, method=method, headers=headers, body=body,
                                       timeout=self.read_timeout),
                        retries=0, on_event=command_event)
            except (BroCommandError, BroCommandOutcomeUnknown, BroPayloadError,
                    BroSessionError) as exc:
                if transport == "page" and isinstance(exc, (BroCommandError, BroPayloadError)):
                    # A terminal observation failure says nothing about
                    # the asynchronous GET still running in the page.
                    # Only its own settled slot permits further work.
                    observation_error = exc
                    exc = BroCommandOutcomeUnknown(
                        "page fetch outcome unavailable after observation failure",
                        session_id=getattr(observation_error, "session_id", None) or self.session.session_id,
                        command_id=getattr(observation_error, "command_id", None),
                        phase="page_fetch_observation", last_status="unknown")
                    exc.page_fetch_poll_failure = getattr(observation_error, "page_fetch_poll_failure", False)
                    exc.__cause__ = observation_error
                if operation_id and not getattr(exc, "page_fetch_poll_failure", False):
                    state = "outcome_unknown" if isinstance(exc, BroCommandOutcomeUnknown) else (
                        "payload_error" if isinstance(exc, BroPayloadError) else "failed")
                    self.operation_store.update_operation(
                        operation_id, state, session_id=getattr(exc, "session_id", None),
                        command_id=getattr(exc, "command_id", None),
                        phase=getattr(exc, "phase", None),
                        last_status=getattr(exc, "last_status", None),
                        error_type=type(exc).__name__)
                    if self.operation_store.get_meta("recordsAtFirstCommandFailure") is None:
                        self.operation_store.set_meta(
                            "recordsAtFirstCommandFailure", self.operation_store.count())
                terminal_timeout = (not getattr(exc, "page_fetch_poll_failure", False)
                                    and isinstance(exc, BroCommandError)
                                    and "timeout" in f"{exc.name} {exc.message}".lower()
                                    and method == "GET" and "comment" in what.lower())
                if terminal_timeout and read_attempt < self.read_retries:
                    info = self.session.client.get_session(self.session.session_id)
                    if str(info.get("status") or "") not in LIVE_STATUSES:
                        raise BroSessionError(
                            "browser was not ready after terminal command timeout",
                            session_id=self.session.session_id,
                            command_id=getattr(exc, "command_id", None),
                            phase="retry_readiness",
                            last_status=str(info.get("status") or "")) from exc
                    if self.account_bound and not self.authenticated:
                        raise FatalError("Authenticated Instagram context is no longer valid") from exc
                    continue
                raise exc
            status = int(_result_of(payload).get("status") or 0)
            if (_result_of(payload).get("pageFetchAborted") or status < 500
                    or read_attempt >= allowed_reads - 1 or method != "GET"):
                break
            # Classify challenge/rate/login payloads before a retry.
            try:
                self._unwrap(payload, url=url, what=what, allow_html=allow_html)
            except InstagramServerError:
                if operation_id:
                    self.operation_store.update_operation(
                        operation_id, "failed", phase="decode", last_status="done",
                        error_type="InstagramServerError")
                continue
            except (RateLimitedError, ChallengeRequiredError, LoginRequiredError) as exc:
                if self.cookies or self.account_bound:
                    raise FatalError(str(exc)) from exc
                raise
        try:
            decoded = self._unwrap(payload, url=url, what=what, allow_html=allow_html)
            if operation_id:
                self.operation_store.save_pending_page(
                    self.operation_job_id, operation_id, "api", decoded)
            return decoded
        except PageFetchReadTimeoutError as exc:
            if operation_id:
                self.operation_store.update_operation(operation_id, "failed",
                    phase="page_fetch_cancelled", last_status="done",
                    error_type=type(exc).__name__)
            raise
        except LoginRequiredError as exc:
            if (self.cookies or self.account_bound) and (self.strict_auth or _result_of(payload).get("status") == 429):
                raise FatalError(str(exc)) from exc
            raise
        except ChallengeRequiredError as exc:
            if self.cookies or self.account_bound:
                raise FatalError(str(exc)) from exc
            raise
        except RateLimitedError as exc:
            if self.cookies or self.account_bound:
                raise FatalError(str(exc)) from exc
            raise

    def _page_fetch(self, url: str, command_event, operation_id: str | None,
                    *, attempt: int = 1, method: str = "GET", body: str | None = None,
                    content_type: str | None = None, web_headers: bool = False) -> dict[str, Any]:
        """Submit one page-context request under a durable key and wait,
        within bounds, for its result. The submit is the journaled command;
        the polls only look at the slot. A request still pending after the
        wait is an unknown outcome -- the slot stays in the page and a resume
        reads it under the same identity instead of fetching again.

        A plain GET (comment pages) is sent exactly as before: bare, with the
        page's cookies. ``web_headers`` adds what the web client itself sends
        (app id, ASBD id, ``x-requested-with``, the page's CSRF cookie and
        www-claim), read inside the page so no token passes through here; a
        POST always carries them -- Instagram refuses a POST without CSRF.
        """
        fingerprint = hashlib.sha256(json.dumps(
            {"method": method, "url": url, "body": body},
            sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        options = None
        if method != "GET" or web_headers or content_type:
            options = {"method": method, "body": body, "contentType": content_type,
                       "web": ({"appId": self.tokens.app_id, "asbd": ep.ASBD_ID}
                               if (web_headers or method != "GET") else None)}
        if method == "GET":
            options = {**(options or {}), "timeoutMs": max(1, int((self.read_timeout or PAGE_FETCH_WAIT) * 1000))}
        if self.operation_store is not None:
            # The same read left pending in earlier invocations (unknown
            # outcome, or deferred because its VM was gone before the slot
            # could be read): Instagram is not answering this request. Past
            # the bound it is a residual gap for the task, not another stop
            # of the whole run. Measured 18 September: one cursor of one post
            # hung past 30 s in three sessions in a row.
            unanswered = self.operation_store.db.execute(
                "SELECT count(*) FROM command_operations WHERE request_fingerprint=? "
                "AND state IN ('outcome_unknown','deferred')", (fingerprint,)).fetchone()[0]
            if unanswered >= PAGE_FETCH_UNANSWERED_LIMIT:
                raise PageFetchUnresponsiveError(
                    f"page fetch left unanswered in {unanswered} invocations; not retried: {url[:120]}")
        key = page_fetch_key(fingerprint, attempt)
        applied_keys: list[str] = []
        if self.operation_store is not None:
            rows = self.operation_store.db.execute("""SELECT request_fingerprint,attempt
                FROM command_operations WHERE operation_kind='page_fetch'
                AND state='applied' AND session_id=?""",
                (self.session.session_id,))
            applied_keys = sorted({page_fetch_key(row[0], row[1]) for row in rows}
                                  - self._cleaned_page_fetch_keys)
        # The submit script is idempotent by construction: a second run of
        # the same key finds the slot and answers 'exists' without fetching.
        submit_command = cmd.run_js(_PAGE_FETCH_JS.replace("__URL__", json.dumps(url))
                       .replace("__KEY__", json.dumps(key))
                       .replace("__APPLIED__", json.dumps(applied_keys))
                       .replace("__FAST_WAIT__", str(PAGE_FETCH_FAST_WAIT_MS))
                       .replace("__OPTS__", json.dumps(options)),
                       out_type="str", timeout=self.read_timeout)
        inline_slot = None
        if callable(getattr(self.session, "run", None)):
            # The DOM marker lets getbro wait for a completed fetch without
            # repeated management calls. A short timer also wakes a slow
            # fetch, so the batch's identity tag still executes; its pending
            # slot is then observed read-only in the usual bounded loop.
            marker = f'html[data-igs-read-settled={json.dumps(key)}]'
            results = self.session.run([
                submit_command,
                cmd.locate(strategy="css", value=marker, state="attached",
                           timeout_ms=PAGE_FETCH_LOCATOR_WAIT_MS),
                cmd.run_js(_PAGE_READ_JS.replace("__KEY__", json.dumps(key)),
                           out_type="str", timeout=self.read_timeout)],
                retries=0, on_event=command_event, idempotent=True,
                raise_on_failure=False)
            submit = results[0] if results else None
            try:
                inline_slot = json.loads((results[-1] or {}).get("result") or "null") if len(results) >= 3 else None
            except (TypeError, ValueError):
                inline_slot = None
            if not isinstance(inline_slot, dict):
                # The submit is acknowledged; observing the same slot cannot
                # issue a second Instagram GET if the locator failed.
                inline_slot = None
        else:
            # A session without batches (a minimal caller or a test double):
            # submit alone; the slot is then observed in the bounded loop.
            submit = self.session.run_one(submit_command, retries=0,
                on_event=command_event, idempotent=True)
        submitted = str((submit or {}).get("result") or "")
        if submitted not in ("submitted", "exists"):
            raise BroCommandError("PageFetchNotSubmitted",
                                  f"page fetch returned {submitted!r}", "run_js",
                                  session_id=self.session.session_id, phase="page_fetch",
                                  last_status="done")
        self._cleaned_page_fetch_keys.update(applied_keys)
        inline_payload = page_fetch_payload(inline_slot)
        if inline_payload is not None:
            if self.operation_store is not None:
                self.operation_store.add_invocation_metric("pageFetchInlineResults", 1)
            return inline_payload
        operation = None
        if operation_id:
            operation = dict(self.operation_store.db.execute(
                "SELECT * FROM command_operations WHERE id=?", (operation_id,)).fetchone())
        return wait_page_fetch_slot(
            self.session, key, operation_store=self.operation_store, wait=self.read_timeout,
            parent_operation=operation, flush=self.operation_flush,
            time_remaining=self.budget.remaining if self.budget else None)

    def rotate_ip(self) -> None:
        """Stop the browser's VM, boot a fresh one (a new residential exit IP)
        and load Instagram on the page the old one was on."""
        if self.cookies or self.account_bound:
            raise FatalError("Authenticated contexts cannot rotate IPs")
        self.rotations += 1
        page = self.current_page
        self.session.restart()
        self.bootstrapped = False
        self.bootstrap(landing=page, force=True)
        log.info("browser moved to bro session %s (swap %d of this browser)",
                 self.session.session_id, self.rotations)

    def _pace_request(self, *, retry=False) -> None:
        if self.budget:
            self.budget.check()
        if (self.cookies or self.account_bound) and self._last_request and self.request_pause:
            remaining = self.request_pause * random.uniform(1.0, 1.3) - (
                time.monotonic() - self._last_request)
            if remaining > 0:
                time.sleep(remaining)
        self._last_request = time.monotonic()
        if self.budget:
            self.budget.take(retry=retry)
        self.api_calls += 1

    def api_get(
        self,
        url: str,
        *,
        referer: str | None = None,
        what: str = "this resource",
        allow_html: bool = False,
        transport: str = "fetch_json",
        page_headers: bool = False,
    ) -> Any:
        """GET a ``/api/v1/`` endpoint from inside the browser.

        Returns the decoded JSON body.

        Raises:
            LoginRequiredError: Instagram served the logged-out shell (on an
                anonymous read, once no VM swap is left).
            RateLimitedError: the read was throttled (idem).
            ChallengeRequiredError / NotFoundError: per the response body.
        """
        return self._send(url, referer=referer, what=what, allow_html=allow_html,
                          transport=transport, page_headers=page_headers)

    def api_post(
        self,
        url: str,
        body: str,
        *,
        referer: str | None = None,
        what: str = "this resource",
        content_type: str = "application/x-www-form-urlencoded",
        transport: str = "fetch_json",
        page_headers: bool = False,
    ) -> Any:
        return self._send(url, method="POST", body=body, referer=referer,
                          what=what, content_type=content_type, transport=transport,
                          page_headers=page_headers)

    def graphql(
        self,
        doc_id: str,
        variables: dict[str, Any],
        *,
        friendly_name: str | None = None,
        endpoint: str = "/graphql/query",
        observed_form: dict[str, str] | None = None,
        pending_source: str = "api",
        pending_transform=None,
        what: str = "this resource",
    ) -> Any:
        """POST a Relay query.  Requires an `lsd` token from the page."""
        if not self.tokens.lsd:
            self.tokens = self.read_tokens()
        if not self.tokens.lsd:
            raise InstagramError("no LSD token available for a GraphQL call")

        if endpoint not in ("/graphql/query", "/api/graphql"):
            raise InstagramError("untrusted observed GraphQL endpoint")
        url = f"{ep.BASE}{endpoint}"
        headers = ep.graphql_headers(
            lsd=self.tokens.lsd,
            csrf=self.tokens.csrf,
            app_id=self.tokens.app_id,
            referer=self.current_page,
            friendly_name=friendly_name,
        )
        body = ep.graphql_body(doc_id, variables, lsd=self.tokens.lsd,
                               base=observed_form,
                               friendly_name=friendly_name)
        self._pace_request()
        if self.traffic:
            self.traffic.managed(url, "POST", body)
        operation_id = None
        if self.operation_store and self.operation_job_id:
            fingerprint = hashlib.sha256(json.dumps(
                {"method": "POST", "url": url, "body": body},
                sort_keys=True, separators=(",", ":")).encode()).hexdigest()
            operation_id = self.operation_store.begin_operation(
                self.operation_job_id, self.operation_kind or what, fingerprint, 1)

        def command_event(state, fields):
            if not operation_id:
                return
            self.operation_store.update_operation(
                operation_id, state, session_id=self.session.session_id,
                command_id=fields.get("command_id"), phase=fields.get("phase"),
                last_status=fields.get("last_status"))
            if state == "recovered":
                for key in ("recoveredCommands", "recoveredCommandsThisInvocation"):
                    self.operation_store.set_meta(
                        key, self.operation_store.get_meta(key, 0) + 1)
            if state in ("recovering", "observation_lost") and self.operation_store.get_meta(
                    "recordsAtFirstCommandFailure") is None:
                self.operation_store.set_meta(
                    "recordsAtFirstCommandFailure", self.operation_store.count())
            if state in ("recovering", "observation_lost", "outcome_unknown") and self.operation_flush:
                self.operation_flush()

        try:
            payload = self.session.run_one(
                cmd.fetch_json(url, method="POST", headers=headers, body=body,
                               timeout=self.read_timeout),
                retries=0, on_event=command_event)
        except (BroCommandError, BroCommandOutcomeUnknown, BroPayloadError,
                BroSessionError) as exc:
            if operation_id:
                state = "outcome_unknown" if isinstance(exc, BroCommandOutcomeUnknown) else (
                    "payload_error" if isinstance(exc, BroPayloadError) else "failed")
                self.operation_store.update_operation(
                    operation_id, state, session_id=getattr(exc, "session_id", None),
                    command_id=getattr(exc, "command_id", None),
                    phase=getattr(exc, "phase", None),
                    last_status=getattr(exc, "last_status", None),
                    error_type=type(exc).__name__)
            raise
        try:
            decoded = self._unwrap(payload, url=endpoint, what=what)
            try:
                durable = pending_transform(decoded) if pending_transform else decoded
            except Exception as exc:
                if operation_id:
                    self.operation_store.update_operation(
                        operation_id, "failed", phase="decode", last_status="done",
                        error_type=type(exc).__name__)
                raise
            if operation_id:
                self.operation_store.save_pending_page(
                    self.operation_job_id, operation_id, pending_source, durable)
            return decoded
        except (RateLimitedError, ChallengeRequiredError, LoginRequiredError) as exc:
            if operation_id:
                self.operation_store.update_operation(
                    operation_id, "failed", phase="decode", last_status="done",
                    error_type=type(exc).__name__)
            if self.cookies or self.account_bound:
                raise FatalError(str(exc)) from exc
            raise
        except InstagramError as exc:
            if operation_id:
                self.operation_store.update_operation(
                    operation_id, "failed", phase="decode", last_status="done",
                    error_type=type(exc).__name__)
            raise

    # -------------------------------------------------------------- helpers --

    def _unwrap(
        self, payload: Any, *, url: str, what: str, allow_html: bool = False
    ) -> Any:
        """Turn a `fetch_json` payload into JSON, or raise a typed error."""
        result = _result_of(payload)
        if not result:
            raise InstagramError(f"empty response from {url}")

        status = int(result.get("status") or 0)
        body = result.get("json")
        text = str(result.get("text") or "")
        # A comment may literally say "challenge_required" or "please wait".
        # Only error metadata, never user content in successful JSON, is a signal.
        lowered = (" ".join(str(body.get(k) or "") for k in (
            "message", "error_type", "checkpoint_url", "challenge"))
            if isinstance(body, dict) else text[:6000]).lower()
        if isinstance(body, dict) and isinstance(body.get("errors"), list):
            lowered += " " + " ".join(str(e.get("message") or "") for e in body["errors"] if isinstance(e, dict)).lower()

        if status == 404:
            raise NotFoundError(f"Instagram returned 404 for {what}")
        # A REST read the logged-in web app no longer makes answers such a
        # session with a page instead of JSON: 429 and the "Page Not Found"
        # HTML, 429 and nothing, or 200 and the app shell. That means "not
        # served here" -- not a throttle, not a login wall -- and the page is
        # where the data is. A JSON answer from the same path keeps its usual
        # meaning ("please wait" JSON is still a rate limit).
        if (self.tokens.web_confirmed and body is None and ep.web_retired(url)
                and (not text.strip() or text.lstrip().startswith("<"))):
            raise EndpointNotServedError(
                f"Instagram does not serve {what} to a logged-in web session "
                f"(HTTP {status}, {'an HTML page' if text.strip() else 'an empty body'} "
                "instead of JSON); the web app reads it from the page")
        if status in (401, 403) and self.strict_auth:
            raise LoginRequiredError(what)

        if any(m in lowered for m in _CHALLENGE_MARKERS) or (
                isinstance(body, dict) and (body.get("challenge") or body.get("checkpoint_url"))):
            raise ChallengeRequiredError(
                f"Instagram asked for a checkpoint while reading {what}"
            )

        # `require_login` outranks the wording of the message.  Endpoints that
        # a logged-out web client is not allowed to call answer with BOTH
        # "Please wait a few minutes before you try again" and
        # `require_login: true` -- an earlier four-IP probe showed that this
        # combination was a login wall, not a recoverable soft throttle.
        # Treat the explicit flag as the stronger signal.
        if isinstance(body, dict) and (body.get("require_login") or "login_required" in lowered or "login required" in lowered):
            raise LoginRequiredError(what)

        if status == 429 or any(m in lowered for m in _RATE_MARKERS):
            raise RateLimitedError(
                f"Instagram rate-limited the session while reading {what}"
            )
        if any(m in lowered for m in _CHALLENGE_MARKERS):
            raise ChallengeRequiredError(
                f"Instagram asked for a checkpoint while reading {what}"
            )

        if result.get("pageFetchAborted") is True:
            raise PageFetchReadTimeoutError(f"Page GET cancelled after its read timeout: {what}")

        if status >= 500:
            raise InstagramServerError(f"Instagram HTTP {status} for {what}")

        if isinstance(body, dict):
            api_status = str(body.get("status") or "").lower()
            message = str(body.get("message") or "").lower()
            if api_status == "fail":
                if "login_required" in message or "login required" in message:
                    raise LoginRequiredError(what)
                if "challenge" in message or "checkpoint" in message:
                    raise ChallengeRequiredError(message or what)
                if "not found" in message or body.get("spam") is None and status == 404:
                    raise NotFoundError(message or what)
                # Instagram errored for a reason of its own; the rendered page
                # is usually still intact, so this is worth falling back on.
                raise InstagramServerError(
                    f"Instagram error for {what}: {message or body}"
                )
            return body

        if isinstance(body, list):
            return body

        # No JSON came back.  A logged-out HTML shell means the endpoint is
        # gated; anything else is an unexpected payload.
        if text.lstrip().startswith("<"):
            if allow_html:
                return {"_html": text, "_status": status}
            if any(marker in lowered.replace(" ", "") for marker in _LOGIN_MARKERS):
                raise LoginRequiredError(what)
            raise LoginRequiredError(what)

        if status >= 400:
            raise InstagramError(f"Instagram HTTP {status} for {what}: {text[:200]}")
        raise InstagramError(f"unexpected response for {what}: {text[:200]}")


def _looks_logged_in(data: dict[str, Any]) -> bool:
    """Decide whether a rendered page belongs to a signed-in viewer.

    Instagram used to embed ``"is_logged_in":true`` and no longer does: a
    verified-live session rendered the signed-in home feed -- follow list,
    account switcher, "Suggested for you" -- while that marker, the
    ``not-logged-in`` class and every ``viewerId`` field were all absent.
    Requiring it declared working sessions dead.

    So the test is inverted: a live session is one where the logged-out shell
    is *not* being served and the viewer cookie is present. `ds_user_id` alone
    is not enough -- it outlives an expired login -- which is why the
    logged-out signals are checked first.
    """
    if (data.get("pageLoaded") is False or data.get("proxyError")
            or data.get("restricted") or data.get("loginForm") or data.get("loginPrompt")
            or data.get("loggedOutClass") or _logged_out_reason(data)):
        return False
    if data.get("loggedInMarker"):
        return True
    return bool(data.get("userId"))


def _logged_out_reason(data: dict[str, Any]) -> str | None:
    """A logged-out page that the viewer cookie alone would pass as live.

    Measured 2026-09-24 (verification/apify-comparison-2026-09-24-auth-posts-
    reels/diagnosis): cookies exported on 9 September landed every page on the
    saved-account screen -- "Continue", "Use another profile", a sign-up link,
    no password field, no ``not-logged-in`` class -- while ``ds_user_id`` was
    still in the jar. Every earlier check passed it as authenticated, and the
    run then read Instagram's logged-out refusals as throttling. The logged-in
    web app never links sign-up and always links Direct.
    """
    if data.get("signupLink") and not data.get("directLink"):
        return "logged_out_landing"
    return None


def _result_of(payload: Any) -> dict[str, Any]:
    """`fetch_json` nests its response under ``result``; tolerate both shapes."""
    if not isinstance(payload, dict):
        return {}
    result = payload.get("result")
    if isinstance(result, dict):
        return result
    return payload if "status" in payload or "text" in payload else {}
