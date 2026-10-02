## IPs and session swaps


Every browser is one bro session with one residential IP and its own bill.
Before any Instagram read the runner checks the route (an IP service, then
Instagram itself) and records the result in `OUTPUT.preflight`.

**Anonymous runs.** `proxyCountry` is optional: without it getbro assigns its
default exit country, the UK. When
Instagram puts a browser's IP behind its login wall, the browser moves to
a new bro session with a new IP and repeats the blocked read there. This applies
to reads Instagram serves logged-out visitors (`web_profile_info`,
`endpoints.ANONYMOUS_READ_PATHS`) and to rate limits; a read that always needs
an account (the profile feed, comments) is not repeated anywhere.
`maxIpRotations` (default **10**, `--max-ip-rotations`) bounds the swaps of the
whole run, all browsers together; `0` turns them off. `OUTPUT.budget` reports
`ipRotations` / `maxIpRotations`; `billing.sessions[].restarts` counts each
browser's swaps (and `lanes[].ipRotations` in a parallel run).

One anonymous IP may serve a few dozen profile reads before the wall, or
meet it on the very first; how many fresh IPs are usable changes from hour to
hour and between countries, so pinning a country can make it worse. For
anonymous runs it pays to vary the country from run to run (a random
`proxyCountry`, or none for getbro's default) rather than keep one. For an
anonymous batch, several browsers at once are more dependable than one browser
swapping sessions; when swaps run out before the targets do, raise
`maxIpRotations`.

**Runs with cookies.** A `sessionid` replayed from many IPs in quick succession
is what account-takeover detection looks for (it got a test account locked
during development). So a run with cookies is pinned to DE, keeps one browser
and one IP per account, never swaps sessions, and injects each account's
cookies exactly once, into its own browser, after the anonymous route check.
The city defaults to Frankfurt; `proxyCity: ""` accepts any DE city, which
the examples use because a single city's pool can be unavailable. An account that lands on Instagram's security check
(`/auth_platform/`, a checkpoint or a password page) is stopped before any
request.

**Complete mode** keeps its browser for the whole checkpoint, anonymous or
not: the command journal is tied to that session, so it never swaps.

---

## What works without a login


| Target | Logged out | With `sessionCookies` |
| --- | --- | --- |
| Profile details | full record via `web_profile_info`; an IP behind the login wall is swapped for a new bro session | full record from the profile page's own response (`PolarisProfilePageContentQuery`, first grid page as `latestPosts`) |
| Profile posts / reels | the short preview inside `web_profile_info` | full records through the grid (`PolarisProfilePostsQuery`) and `media/{pk}/info`, bounded by `resultsLimit` and `gridMaxScrolls` |
| Single post | caption, author, media and likes from the embed page | full record via `media/{pk}/info` |
| Hashtag / place posts | URL, type and view count through the vision fallback; `--enrich` adds caption, author, likes and media | full records via the API |
| Hashtag / place details | name and post count from the rendered header | full record |
| Mentions (tagged posts) | not possible | the Tagged tab plus `media/{pk}/info` per tile |
| Comments | not possible | standard REST paging, or complete mode (above) |
| Search | literal resolution (`dataSource: "direct"`): a hashtag query becomes that tag, a profile query is verified as a handle; `place` queries fail | ranked results via `/web/search/topsearch/`; `searchType: user` uses `fbsearch/account_serp`, which returns more profiles than the blended endpoint |
| Stories | not possible | full records |

Comments, mentions and stories fail fast without a session, with an
explanation, instead of paying for a vision pass over a page that has nothing
to read: logged out, the post page renders no comment list and `/tagged/` and
stories redirect to the login screen.

The anonymous REST profile feed (`/api/v1/feed/user/`) refuses logged-out
callers (`401`, `require_login: true`), and the logged-out GraphQL grid query
clamps its page size and rejects its own cursor. That is why anonymous posts
stop at the preview. A logged-in grid keeps loading on scroll; it is
virtualised (only a window of tiles stays in the DOM), so the scraper unions
the shortcodes after every scroll step and rebuilds each post from the media
API. Those records carry caption, likes, comments and timestamp, which the
vision fallback cannot read, and cost less.

With `anonymousProfileIds: true`, a run with cookies resolves username targets'
numeric ids anonymously before the cookies go in. It is off by default: a
logged-in web session reads profiles from their page.

---

## Several browsers at once


- **Anonymous:** `broConcurrency: N` (`--concurrency N`) starts N browsers.
- **Several accounts:** `sessionCookiesList` (or `--cookies-file` repeated)
  adds accounts to `sessionCookies`; every account gets exactly one browser,
  and `authenticatedRequestPause` paces each account on its own. The same
  account twice is refused.

The browsers take targets from one queue, so a faster browser takes more of
them. A browser whose queue is empty stops its bro session at once; it never waits for
the others. A browser whose account is refused, throttled or challenged, or
whose session dies, stops at once; the target it was on goes to a browser that is
still working if it had not produced a record yet (at most twice). What no
browser got to is listed in `OUTPUT.targetsNotScraped`. `maxApiRequests`,
`maxRunSeconds` and `maxIpRotations` are the whole run's. A search runs in the
first account's browser, which then joins the queue. `OUTPUT.lanes` reports
each browser: account id, status and reason, targets done / partial / failed /
handed back, records, managed requests, session swaps, startup and work seconds.

In complete mode each account keeps one browser and one checkpoint
(`accounts/<n>/`); posts are handed out one at a time, the next post to
whichever browser is free first, and the run's dataset is the union of the
checkpoints (`OUTPUT.shares`). One post is never split between accounts. See
[COLLECTION_MODES.md](COLLECTION_MODES.md#multiple-accounts).

What that means in practice:

- Anonymous batches gain the most: every browser brings its own IP, so
  the login wall that stops one IP does not stop the batch.
- Accounts shorten work spread over many targets or posts. They do not speed
  up one post in complete mode: a post's comments are read by one account.
- Every extra browser pays its own start-up and its own bill, so a short run
  gains little.
- A browser's speed also depends on getbro's command queue, which varies. To
  judge a speed-up, compare with a one-browser run made at the same time.

---

