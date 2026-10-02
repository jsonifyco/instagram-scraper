# Collection modes

### `collectionMode: "standard"` (default)

Targets are scraped one after another, records stream into the dataset, and
the run is not resumable. Every `resultsType` works this way; comments are
read through the REST comment endpoints of each post
(`resultsLimit` comments per post, replies with `includeNestedComments`).

### `collectionMode: "complete"`

`collectionMode: "complete"` saves everything collected into a transactional SQLite checkpoint (`state.sqlite`): records, cursors, job queues, and the log of every getbro command. A stopped run can be resumed via `--resume <directory>`; a process crash does not lose what has been saved. All `resultsType` work, but the primary purpose of the mode is the complete collection of comments. Comments require account cookies.

## Launch and resume

From the project root after `python -m pip install -e .`:

```powershell
python -m instagram_scraper --input examples/comments_complete.json --cookies-file .cookies.json
python -m instagram_scraper --resume storage/comments-complete --cookies-file .cookies.json
```

`examples/comments_complete.json` — all comments of a single post using chronological model with replies: lite, any DE city, 2000 managed reads, 3300 seconds of collection out of 3600 seconds of browser lifetime.
`examples/comments_complete_12_posts.json` — a verified profile of 12 posts in one browser (8000 reads, 21000 seconds of collection, 21600 seconds of lifetime).
`examples/comments_complete_ranked.json` — ranked model.

Every new launch requires a new directory (`outputDir`). Resume restores the saved input without secrets. If the previous bro session is still alive, an attach is performed without passing cookies again. If the session is already stopped, cookies from the current input are required: secrets from the checkpoint are not restored. Changing the account relative to the saved viewer is prohibited. There is no automatic browser replacement within a run: a new bro session is created only on the next explicit resume. During a graceful shutdown, the session is always stopped.

Limits can be increased on resume (e.g. `--limit 20000`); the uncompleted remainder of the page is preserved. Changing targets, sorting, result type or dates requires a separate directory. Feed responses are saved along with cursors, so an interrupted post search is replayed from SQLite without requests to Instagram.

For the Python API, the `result.items` list is materialized by default; `materializeItems: false` leaves it empty (the number of records is in `len(result)` and `result.stats.items`, the data is in `result.dataset_path`). The CLI materializes records only with `--print`.


## Comment models

### Chronological (`isNewestComments: true`), recommended

The list of parents is read as REST `sort_order=recent`; the head cursor is continued within the page (page-fetch), reply threads — via REST and page-fetch. There is no UI traversal, observer, and HAR. The model reaches the server terminal of the list: 3387 parents in 41 minutes ($0.28) on a post with 4.4k comments (2026-09-18). `onlyPostsNewerThan` stops the list at the date boundary (`date_floor`).

**One post document (default, `reusePostDocument: true`).**
The page of the first post is opened once, comments of all subsequent posts are read from this exact document by their media ID. There is no navigation to each post's page, `deepCollection.postNavigationsAvoided` counts the saved navigations. A probe on Sep 28 showed that reading a different media ID from another document returns the same pages as from its native one (overlaps 12/12, 14/14, 15/15, 15/15, 14/14). Measurement on Sep 29: 12 posts, **15 840 unique records in one browser in 3h 17m, $0.48**, 1730 managed reads, all 12 parent lists and 640 reply threads reached the server terminal ([result](verification/session-throughput-2026-09-28/DEEP-ONE-VM-RESULT-2026-09-29.md)).

**Separate page per post (`reusePostDocument: false`, `--no-reuse-post-document`)** — legacy behavior: before reading each post, its page is opened. Runs using this method stopped after the eighth post (12 × 100: 827 records), the best result of a single browser was 8781 records in 146 minutes.

### Ranked (`isNewestComments: false`)

The `popular` order of the web interface, traversed via the page. Visible threads are expanded until the next list movement: one click, waiting for the observer or HAR response, saving and applying form a single step. Then the traversal moves to the next confirmed parent or shifts by 70% of the checked area's height. The absence of the button is checked twice more, but does not prove completeness; only a fresh terminal response proves it. UI steps do not consume `maxCommentPages`. Around 500 parents per hour on a large post.

After the terminal parents page, the remaining unexamined IDs pass through `audit_gaps`. A parent present in the DOM is focused and processed; one that never appeared in the current document receives `not_rendered`, and its unfinished thread is queued in `direct_ui`: the scraper opens the exact `/p/<code>/c/<parent>/`, checks the parent and its own control, then expands the thread. Formats `View replies (N)` and `View all N replies` are supported. If the first click of the direct page returns a REST page with a head cursor, the thread continues with managed `min_id` reads with `is_chronological=true&paging_direction=view_more` parameters.
An ambiguous control is not clicked. A row that was previously present but lost before examination remains a real `scan_gaps`. The history of the run and document is stored in SQLite: ID source, presence in the DOM, viewport intersection, and the presence of the replies control.

`revisitRankedParentsOnResume: true` (or `--revisit-ranked-parents-on-resume`) starts a new parents pass on explicit resume: Instagram's ranked sample differs between sessions. Saved IDs are not duplicated.


## Comments priority and enrichment

`collectionPhase: "all"` (default) first performs the main traversal of all posts and threads, then enrichment. If blocked lists, unopened targets remain, or the results limit is reached, automatic enrichment is deferred. `collectionPhase: "collect"` saves enrichment tasks for a separate run.

`expandOwners: false` excludes profile requests; owner fields obtained together with comments are preserved. With `true`, tasks are cached by owner. `maxEnrichmentRequests` limits actual enrichment API requests, including retries and fallback. `0` disables them, the absence of the parameter leaves only the overall limit. Each such request also consumes `maxApiRequests`, the overall `maxRunSeconds` continues to apply.

A separate phase works with saved records and does not traverse posts:

```powershell
python -m instagram_scraper --resume storage/comments-complete --cookies-file .cookies.json --collection-phase enrich --expand-owners --max-enrichment-requests 10
```

The route, intervals, attach, and single browser rules are the same. Cursors of the main traversal are not modified, completed jobs and cache are not requested again. To return to collection, specify `--collection-phase collect` on the next explicit resume.

On 429, checkpoint, or authorization failure, the entire session stops, even during enrichment. Data and unfinished job are saved; the traversal does not continue via another endpoint or IP. Exhaustion of the enrichment limit alone leaves its queue to continue (`enrichmentStopReason`); the requests counter is in `budget.enrichmentApiRequests`.


## Route and browser lifetime

The browser is created with a route check: first the IP service, then Instagram. An IP service error allows one anonymous Instagram check. Cookies are passed only after success, once, and to the same session. A run with cookies is pinned to DE (default city Frankfurt, `proxyCity: ""` — any DE city; examples use it because the Frankfurt pool answered 502 in September). The IP check confirms the ability to get an external IP, but is not an independent geolocation attestation. Complete mode never changes the bro session, including anonymously: the command log is bound to its session.

`broSessionTimeout` accepts up to 21600 seconds (6 hours, getbro limit), default 3600. Separately from the getbro lifetime, the session terminates after 300 seconds without running and pending commands; keepalive is not sent. Keep `maxRunSeconds` below the browser lifetime, so that the stop and final billing fit within the browser's life. Complete mode always uses `proxyPolicy: "full"`.


## Multiple accounts

Cookies of multiple accounts (`sessionCookies` and `sessionCookiesList`, in CLI — multiple `--cookies-file`) split the targets between accounts. Each share is a separate checkpoint `accounts/<n>/` with its own browser (bro session), command log, and proxy exit; account cookies are passed once and only to its browser. Shares work simultaneously, inside a share the engine remains single-browser with all recovery guarantees. Posts are not divided in advance: each browser takes one post at the beginning, the next post is taken by the browser that became free first, in the same session and in the same checkpoint. The accounts plan is saved in the run's checkpoint.

```powershell
python -m instagram_scraper --input run.json --cookies-file a.json --cookies-file b.json --output-dir storage/two-accounts
python -m instagram_scraper --resume storage/two-accounts --cookies-file b.json --cookies-file a.json
```

- The dataset of the run is a union of shares in the input order; live export of each share is in its `accounts/<n>/datasets/default/`. `OUTPUT.shares` shows per share the account, status, records, managed reads, cost, and stop reason.
- `maxApiRequests` and `maxRunSeconds` are the overall budget of the run; `OUTPUT` of a share counts only its own reads.
- Stopping a share (429, checkpoint, lost session) does not stop the others. The post on which it stopped remains in its checkpoint; not yet taken posts are grabbed by other accounts. When all shares finish, the run ends with the error of the stopped share; what it collected remains in the dataset. `OUTPUT.targetsNotScraped` lists posts that no one took.
- On resume, a share is returned to its account by id in cookies, file order doesn't matter. First, the share finishes its posts, then takes new ones. A share whose cookies are not passed is skipped, its previous records remain. A checkpoint collected by one account is not divided on resume.
- `directUrls` are needed (search is not split) and the account id in cookies (`ds_user_id` or start of `sessionid`).
- A single post is not accelerated: its comments list is read by one account.

---

## Internal architecture & diagnostics

For low-level mechanics — including transactional getbro command recovery, observer vs. HAR response capture, native GraphQL adapters, SQLite checkpoint state schemas, coverage calculation, and test suites — see [Complete Mode Architecture in ARCHITECTURE.md](ARCHITECTURE.md#complete-mode-architecture).
