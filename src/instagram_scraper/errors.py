"""Exception hierarchy for the Instagram scraper.

Every failure that the scraper can recover from (retry, fall back to another
strategy, skip the URL) derives from :class:`ScraperError`.  Failures that mean
"stop the whole run" derive from :class:`FatalError`.
"""

from __future__ import annotations


class ScraperError(Exception):
    """Base class for every error raised by this package."""


class FatalError(ScraperError):
    """Unrecoverable: the run must stop (bad credentials, no balance, ...)."""


# --------------------------------------------------------------------------- #
# getbro transport
# --------------------------------------------------------------------------- #

class BroError(ScraperError):
    """Something went wrong while talking to the getbro API."""

    def __init__(self, message: str, *, session_id: str | None = None,
                 command_id: str | None = None, phase: str | None = None,
                 last_status: str | None = None) -> None:
        self.session_id = session_id
        self.command_id = command_id
        self.phase = phase
        self.last_status = last_status
        context = ", ".join(f"{key}={value}" for key, value in (
            ("sessionId", session_id), ("commandId", command_id),
            ("phase", phase), ("lastStatus", last_status)) if value is not None)
        super().__init__(f"{message} ({context})" if context else message)


class BroAuthError(FatalError):
    """The getbro API key is missing, invalid or out of balance."""


class BroHTTPError(BroError):
    """Non-2xx response from api.getbro.ws."""

    def __init__(self, status: int, body: str, url: str = "", **context) -> None:
        self.status = status
        self.body = body
        self.url = url
        super().__init__(f"getbro HTTP {status} for {url}: {body[:400]}", **context)


class BroSessionError(BroError):
    """The remote browser session died, never became idle, or was cancelled."""


class BroCommandError(BroError):
    """A command batch finished with status ``failed``/``cancelled``."""

    def __init__(self, name: str, message: str, command: str = "", **context) -> None:
        self.name = name
        self.message = message
        self.command = command
        super().__init__(f"command {command or '?'} failed: {name}: {message}", **context)


class BroSubmitNotAccepted(BroError):
    """No matching command was listed after a lost submit response.

    This is not proof that the API never accepted it. The caller may resend
    only a batch explicitly classified as idempotent.
    """


class BroTimeoutError(BroError):
    """A command did not reach a terminal state within the client-side budget."""


class BroCommandOutcomeUnknown(BroTimeoutError):
    """The command may still be running, so issuing another command is unsafe."""


class BroPayloadError(BroError):
    """A completed command's offloaded result could not be downloaded."""


# --------------------------------------------------------------------------- #
# Instagram layer
# --------------------------------------------------------------------------- #

class InstagramError(ScraperError):
    """Base class for Instagram-specific problems."""


class LoginRequiredError(InstagramError):
    """Instagram served a login wall for an endpoint that needs a session."""

    def __init__(self, what: str = "this resource") -> None:
        super().__init__(
            f"Instagram requires an authenticated session to read {what}. "
            "Provide `sessionCookies` in the input (or set IG_SESSIONID) "
            "or enable `aiFallback` to scrape the rendered page instead."
        )


class NotFoundError(InstagramError):
    """Profile / post / hashtag / place does not exist (or was removed)."""


class PrivateProfileError(InstagramError):
    """The profile exists but its media are not visible to the viewer."""


class RateLimitedError(InstagramError):
    """Instagram answered 429 or the equivalent checkpoint response."""


class PageFetchUnresponsiveError(InstagramError):
    """The same page-context read stayed pending past the wait in several
    invocations: Instagram does not answer that request. The task is
    classified as a residual gap instead of stopping the run again."""


class PageFetchReadTimeoutError(InstagramError):
    """A page GET rejected after our AbortController cancelled it.

    Unlike an observation timeout, this is a confirmed local terminal outcome.
    Its cursor can be deferred while other lists run in the same browser.
    """


class InstagramServerError(InstagramError):
    """Instagram failed the request for a reason of its own.

    Not a login wall, not a rate limit, not a missing resource -- the API
    simply errored. Observed in the wild on @natgeo, whose `web_profile_info`
    answers HTTP 400 with "Asset asset://laser.provider/... has been deleted.
    You cannot use this schema" while the profile page itself renders fine.

    Because the fault is server-side and the rendered page is usually intact,
    this is worth falling back on rather than giving up on the target.
    """


class EndpointNotServedError(InstagramServerError):
    """Instagram does not serve this REST read to a logged-in web session.

    Measured 2026-09-29 on two accounts: ``usertags/{id}/feed`` and
    ``users/web_profile_info`` answered HTTP 429 with the HTML "Page Not
    Found" page of the logged-in app, ``users/{id}/info`` 429 with an empty
    body -- on the first read of the session, the same minute ``topsearch``
    and ``reels_media`` answered 200. Not a rate limit: the web app itself
    reads that data through GraphQL on the page. The account needs nothing;
    the scraper reads the page instead (see ``endpoints.WEB_RETIRED_PATHS``).
    """


class ChallengeRequiredError(InstagramError):
    """Instagram asked the session to solve a checkpoint / challenge."""


class ExtractionError(InstagramError):
    """Every strategy in the resolution chain failed for one target."""


# --------------------------------------------------------------------------- #
# Input validation
# --------------------------------------------------------------------------- #

class InputError(FatalError):
    """The actor input is malformed."""


class UnsupportedUrlError(InputError):
    """A `directUrls` entry is not a recognisable Instagram URL."""
