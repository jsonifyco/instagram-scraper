## How a request is resolved


Each target walks a chain and stops at the first layer that produces data.
Every record carries `dataSource`, naming the layer that produced it.

```
1. private web API   /api/v1/..., page GraphQL   dataSource: "api"
        │  gated behind a login (anonymous: after the session swaps), or not served
        ▼
2. grid harvest      scroll + media API          dataSource: "api"   (profiles, with a session)
        │  no session, or the grid did not load
        ▼
3. rendered header   <h1> / <title> / og tags    dataSource: "page"  (tags, places, profiles)
        │  the page carries no header
        ▼
4. embed renderer    /p/<code>/embed/            dataSource: "embed" (single posts)
        │  no embed for this target type
        ▼
5. vision fallback   getbro `extract`            dataSource: "ai" (or "ai+embed" with --enrich)
        ▼
6. clear failure     LoginRequiredError with what to supply
```

Layers 3 and 4 cost no AI tokens: tag and place pages server-render their name
and post count even for a logged-out visitor. The vision fallback (`aiFallback`,
default on) is the slow, expensive last resort; turn it off with `--no-ai` when
thin records are worse than none.

A logged-in web session is not served some REST reads the web app no longer
makes (`feed/user`, `clips/user`, `usertags/{id}/feed`, `users/{id}/info`,
`web_profile_info`: a 429 with the "Page Not Found" HTML, or an empty body).
The scraper does not call them for such a session and reads the profile page
and its tabs instead; an answer of that kind is `EndpointNotServedError`, not a
rate limit.

---

## Architecture


```
src/instagram_scraper/
  bro/            getbro transport
    sdk_client.py   every wire call through bro-api-sdk; bounded session create
    client.py       recovery by command id, batch identity tags, offloaded payloads
    session.py      one managed browser: start, restart (anonymous only), billing at stop
    commands.py     command builders (open_url, fetch_json, run_js, extract, ...)
  ig/             Instagram access
    endpoints.py    every URL and header; web-retired and anonymous-read path lists
    context.py      bootstrap, tokens, response classification, page-context reads, session swaps
    api.py          typed endpoint calls with cursor pagination
    page.py, grid.py, embed.py   rendered headers and profile page, grid harvest, embed renderer
    network.py, observer.py, native_comments.py, comment_pages.py   comment sources and paging
    ai.py           vision fallback
  mappers/        raw Instagram JSON -> Apify records
  scrapers/       one per resultsType, each owning its fallback chain; search discovery
  runner.py       targets -> browsers -> dataset; route check, cookies, teardown
  pool.py         parallel browsers: shared target queue, hand-back, shared profile ids
  shares.py       complete mode with several accounts: the plan and the merged shares
  complete.py     resumable collection: comment models, queues, enrichment
  state.py        SQLite checkpoint, work queue, atomic exports
  budget.py       the run's limits: managed reads, time, session swaps
  preflight.py    anonymous route check before any cookie
  storage.py, traffic.py, input_model.py, urls.py, shortcode.py, errors.py, cli.py
```

Two details worth knowing. **Shortcodes are converted offline:**
`DcOX3hWFiey` and `3967213292204992434` are the same media id in different
bases, so a `/p/<code>/` URL reaches `media/<pk>/info` with no lookup
(`shortcode.py`). **Requests run inside the page:** `fetch_json` and the
page-context reads execute in the live tab, so every call carries the session's
cookies, proxy route and TLS fingerprint.

---

## Complete mode architecture

### Restoration of getbro commands

Every managed GET of comments is logged in `state.sqlite`: the intent is fixed before submit, then `sessionId`, `commandId`, phase, and last status are saved. Request parameters are represented by a SHA-256 fingerprint; cookies, secret headers, and signed URLs of offloaded payloads are not saved.

`broRecoveryTimeout` (default 120 seconds) limits one recovery episode. On temporary 404, transport error, or observation timeout, the client reads the same `commandId` and checks the state of the same bro session via normal getbro user endpoints. An error reading the session status does not prove the session is dead. As long as the command remains `pending` or its outcome is unknown, new fetch, navigation, and keepalive are not sent. After budget exhaustion, the run saves `command_outcome_unknown` and stops. `broDispatchTimeout` is a diagnostic threshold: a command remaining `pending` longer than this is logged once and in `stalledDispatches` and continues to be observed under the same command ID.

A successful command with a temporarily unavailable offloaded payload redownloads the result and rereads the envelope of the same `commandId`; the Instagram request is not repeated. The terminal timeout of a GET comments can be retried once in the same ready session: the retry consumes `maxApiRequests`, observes the normal interval, and is limited by `maxRequestRetries` (default 1). After the second failure, the task is deferred, other threads continue; jobs deferred by a command failure get one more pass in the same session. Three consecutive reads without success terminate the run.

A decoded page is first saved in `pending_pages`. Comments, new cursor, and page application mark are committed in a single transaction. Resume first applies such pages without network and resolves the unfinished `commandId` before any new browser command. If the previous bro session has already finished, the unfinished operation of the old session is marked as deferred.


### Technical details of comment models

#### Page responses source (`responseSource`)

- `observer` (default) — `fetch`/`XMLHttpRequest` interceptor inside the page; the queue is read and confirmed after saving pages. HAR is captured only for the initial load and a one-time verification — with the same resource types filter as `har_filtered`. If the observer is unavailable (did not install, stopped responding, document changed, lost events, failed verification, did not see the page after pagination or response after click), the run does not stop: the source becomes `har_filtered`, the fact and reason are recorded in `responseSource.fallback` (`effective: har_filtered`), the cumulative archive covers everything the queue did not deliver, and fingerprints reject already saved pages.
- `har_filtered` — cumulative `dump_har_logs` after each step, narrowed down by the `resource_types` parameter to `document`, `xhr`, `fetch`, `other`. A controlled check on September 15 (`verification/archive-2026-09-30/tools/har_since_probe.py`) showed that getbro categorizes `fetch()`, `XMLHttpRequest` and its own `fetch_json` to the `other` type, so without it no comments page is exported; scripts, styles, images, media and fonts are not exported. A request that started before the export and finished after it appears fully only in the next cumulative export. The interceptor in this mode works as a witness: after each dump, the comparison by IDs, cursors, and terminal responses is written to `responseSource.harObserverComparison`, the duration and size of the dump — in `harDumpLog`.
- `har` — the same cumulative export without the resource types filter.

Late HAR responses are not discarded by start time; processed versions of bodies are deduplicated by fingerprints in SQLite.

#### Native adapter (experimental)

The adapter accepts only an observed comment read request: numeric `doc_id`, operation name Comment/Comments Query, suitable variables structure, media ID match, cursor slot, and `edges/page_info` connection. Secret headers, cookies, and CSRF tokens are not saved in SQLite; tokens for continuation are taken from the current browser context. Arbitrary POST and Mutation are not reproduced. In the absence of a suitable request, REST works; if the operation changes, the list switches to REST while preserving records, and the reason remains in the job (`nativeFallbackReason` / `nativeFallbackDetail`). `nativeValidation.parents` and `.replies` become true only after a confirmed continuation of the corresponding native list.

### Under the hood

| Component | Behavior |
| --- | --- |
| Budgets | Shared `maxApiRequests` and `maxRunSeconds` per explicit run. GET retries, enrichment, and native requests consume a single budget. Transport timeouts are limited by the remaining time; stop and final billing are executed even after budget exhaustion. |
| State | SQLite: records, field origin, pages, cursors, queues, targets, bro session ID, and checked viewer. Page recording and cursor advancement are a single transaction. The remainder of a page at limit is saved fully. |
| Export | JSON, JSONL, and CSV are read iteratively from SQLite. Background export every 30 seconds uses a separate consistent DB snapshot and atomic file replacement; background export failure is logged, the next export retries. A final export is performed on completion. |
| Comments | REST and observed GraphQL Query are merged by post, comment, and parent IDs. Independent head and tail REST cursors are supported. Previews do not close the list. |
| Queue | Cycle: parents page, up to three pages of different reply threads; interleaving works across posts. Enrichment starts after all lists are completed. |
| Field preservation | A correctly identified incomplete record is preserved; enrichment does not consume the unique records limit. Zero counters and `false` are preserved, sparse responses do not erase data, older observations do not overwrite fresh ones. |
| Owners and media | Shared queue and owner profile cache by ID; `expandOwners` adds an extended owner block. Media, music, GIFs, carousels, and geolocation go through mappers. |
| Streamed feed | Grid serves batches as you scroll. Feed/clips/tagged/sections and profile pages are saved in SQLite and reused after interruption. |
| Compatibility | `enhanceUserSearchWithFacebookPage` is not supported (summary indicates this); `expandOwners` outside complete mode is an explicit error. |


### Artifacts and summary interpretation

- `state.sqlite` — internal state, deduplication, queues, and response cache.
- `datasets/<datasetName>/items.json|jsonl|csv` — atomic export of the current snapshot.
- `key_value_stores/<datasetName>/INPUT.json` — configuration with redacted secrets.
- `key_value_stores/<datasetName>/OUTPUT.json` — result, load, and diagnostics.

In `records.fields`, a distinction is made between `not_requested`, `empty`, `unavailable`, and `value`; observations have `source` and `observedAt`. An unavailable field can be populated from the next response.

`coverage` separately shows parents, replies, unfinished lists, unfinished enrichment, stop reasons, and field state. `traversal` can be `not_started`, `partial`, and `source_exhausted`. The latter means the exhaustion of observed lists, not a proof of 100% of Instagram comments. Instagram's counters are a diagnostic indicator (on 12 posts, 19,033 was declared for 15,840 collected; all lists reached the server terminal). Reaching the limit, a lost cursor, or an unfinished post search are not declared as a complete traversal.

`budget` accounts for managed Instagram requests; getbro navigation commands, scrolling, and background browser requests do not consume `maxApiRequests`, but consume time. `traffic` separately counts requests that ended up in HAR, correlates managed requests, and flags the remaining traffic; in the chronological model, HAR is not captured, and traffic statistics are marked as partial. Full HAR and its secret headers are not saved.

`metrics` contains new unique records for the current launch, requests per 100 new records, and time to the first new record — separately from records accumulated before resume. `billing.sessions[].confirmedStopped` confirms the session has stopped; without confirmation, the information is not presented as final billing.

`deepCollection` shows new and cumulative numbers of parents and replies, confirmed and deferred threads, clicks, list steps, UI/HAR time, post loads and reopens, saved navigations (`postNavigationsAvoided`), and pages applied after restoration.


### Verification

```powershell
.venv/Scripts/python.exe -m pytest -q -o addopts='' --basetemp=<new directory>
```

Use a new `--basetemp`: pytest clears the selected temporary directory.
The suite checks transactional rollback, page replay, buffer after limit, corrupted cursor, native continuations and REST fallback, both comment models and the one-document mode, queues between threads and posts, fields and zero values, owner cache, reading pages from SQLite again, attach, budgets, route validation, stopping and billing, account shares. Export is tested on 10,000 synthetic records. As of Sep 30, 2026, the entire suite is 849 tests.

