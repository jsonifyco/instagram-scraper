## Input reference


Field names match `apify/instagram-scraper`; both `camelCase` and `snake_case`
are accepted, and unknown keys are ignored.

### Apify-compatible

| Field | Type | Default | Notes |
| --- | --- | --- | --- |
| `directUrls` | string[] | `[]` | Profiles, posts, reels, hashtags, places, `/tagged/`, `/reels/`, stories. A bare `@handle`, `nasa` or a numeric user id (resolved with one request) works too. Aliases: `startUrls`, `urls`. |
| `resultsType` | string | `posts` | `posts`, `reels`, `comments`, `details`, `mentions`, `stories`. |
| `resultsLimit` | int | `200` | Per URL; for comments, per post. |
| `search` | string | `""` | Comma-separated queries; used when `directUrls` is empty. Hits become targets whose records carry `searchTerm` / `searchSource`; the hits themselves go to `key_value_stores/<datasetName>/SEARCH_HITS.json`. |
| `searchType` | string | `hashtag` | `hashtag`, `user` (alias `profile`), `place`. |
| `searchLimit` | int | `10` | Hits per query. |
| `onlyPostsNewerThan` | string | — | `"3 days"`, `"2 months ago"`, `"2026-01-31"`, ISO timestamps, unix epochs. Applies to posts and, by each comment's timestamp, to comments. Alias `fromDate`. |
| `untilDate` | string | — | Upper bound, same formats. Alias `toDate`. |
| `addParentData` | bool | `false` | Adds `fromProfile` / `fromHashtag` / `fromPlace`. |
| `skipPinnedPosts` | bool | `false` | |
| `isNewestComments` | bool | `false` | Newest first; in complete mode, the chronological model. |
| `includeNestedComments` | bool | `false` | Replies become separate records. |
| `addProfileStatistics` | bool | `false` | Extra category/contact fields on `details`. |
| `enhanceUserSearchWithFacebookPage` | bool | `false` | Accepted, not supported: the run output says so. |

### Accounts

| Field | Type | Default | Notes |
| --- | --- | --- | --- |
| `sessionCookies` | string \| object[] | — | A `sessionid`, a `"a=b; c=d"` header or an array of cookie objects; also `$IG_SESSIONID`. The run's first account. Alias `cookies`. |
| `sessionCookiesList` | string \| array | — | Further accounts, one entry each in any `sessionCookies` form (or one string: a JSON array, or one account per line). |
| `requireValidSession` | bool | `true` | Stop when cookies were given but Instagram serves the logged-out page (`--allow-degraded` turns it off). |
| `resumeSavedLogin` | bool | `true` | Press "Continue" once on Instagram's saved-account screen; nothing is ever typed. |
| `anonymousProfileIds` | bool | `false` | With cookies: resolve username ids anonymously before the cookies go in. |
| `authenticatedRequestPause` | number | `3` | Minimum seconds between request starts of one account, times a 1.0-1.3 jitter. |

### Collection

| Field | Type | Default | Notes |
| --- | --- | --- | --- |
| `collectionMode` | string | `standard` | `standard` or `complete` (see [Collection modes](COLLECTION_MODES.md)). |
| `reusePostDocument` | bool | `true` | Complete chronological comments: read every post from one opened post document; `false` opens each post's page. No effect elsewhere. |
| `collectionPhase` | string | `all` | Complete mode: `all`, `collect`, or `enrich` (needs `resume`). |
| `resume` | string | — | A complete-mode directory to continue (`--resume`). |
| `revisitRankedParentsOnResume` | bool | `false` | Ranked model: start a fresh parent pass on an explicit resume. |
| `responseSource` | string | `observer` | Ranked model: `observer` (in-page observer, filtered HAR as fallback), `har_filtered` or `har` (unfiltered HAR dumps). |
| `expandOwners` | bool | `false` | Complete mode: read each comment owner's profile. |
| `maxEnrichmentRequests` | int | unset | Cap on enrichment reads, retries included; `0` disables enrichment. |
| `maxCommentPages` | int | `100` | Pages per parent list or reply thread. |
| `maxRepliesPerComment` | int | unset | Cap on reply records per parent; `0` fetches no replies. |
| `commentPostLimit` | int | unset | Posts to discover when comments come from a profile, tag or place. |
| `captureNetworkComments` | bool | `false` | Legacy comments: open each post once and reuse the JSON the page received before paging. |
| `captureNetworkResponses` | bool | `true` | Reuse post JSON received while scrolling a grid when its core fields are present. |
| `gridScrollPause` | number | `2` | Seconds after each grid scroll step. |
| `gridMaxScrolls` | int | `60` | Scroll steps per grid harvest. |
| `mediaLookupPause` | number | `1.2` | Seconds between per-post `media/info` lookups, jittered. |

### Limits

| Field | Type | Default | Notes |
| --- | --- | --- | --- |
| `maxApiRequests` | int | unset | Managed Instagram reads for the whole run, retries and enrichment included. |
| `maxRunSeconds` | number | unset | Elapsed-time limit; complete mode saves pending work. |
| `maxRequestRetries` | int | `1` | Re-executions of a safe GET after a terminal getbro timeout or an Instagram 5xx. |
| `maxIpRotations` | int | `10` | Session swaps the anonymous browsers of a run may make behind a login wall; `0` disables; forced to 0 with cookies. |

### Browser and proxy (getbro)

| Field | Type | Default | Notes |
| --- | --- | --- | --- |
| `broApiKey` | string | `$BRO_API_KEY` | |
| `broConcurrency` | int | `1` | Anonymous browsers at once. With cookies it is the number of accounts. Complete mode: one per account. Alias `concurrency`. |
| `proxyTier` | string | `basic` | `lite`, `basic`, `premium`. The complete-mode examples use `lite`. |
| `proxyPolicy` | string | `extended` | `html_only`, `basic`, `extended`, `full` (complete mode always uses `full`). |
| `proxyCountry` | string | — | ISO code for anonymous runs; unset = getbro's default exit (UK). Runs with cookies are pinned to `DE`. |
| `proxyCity` | string | `Frankfurt` on DE | On a DE route: unset = Frankfurt, `""` = any DE city. No default city elsewhere. |
| `blockUnproxied` | bool | `false` | Passed to getbro. |
| `broSessionTimeout` | number | `3600` | Browser lifetime in seconds, up to 21600 (6 h). getbro also stops a browser after 300 s with no command. |
| `broReadTimeout` | number | `90` | Seconds getbro lets one Instagram read run (0.1-180). |
| `broRecoveryTimeout` | number | `120` | Budget for resolving one uncertain command by its id; nothing is re-submitted. |
| `broDispatchTimeout` | number | `45` | A command queued longer is logged as a slow dispatch (diagnostic). |
| `broCommandTimeout` | number | `300` | Wait for one command batch. |
| `broSkipBalanceCheck` | bool | `false` | Skip the local balance precheck (development accounts). |
| `broBaseUrl` | string | `https://api.getbro.ws` | Also `$BRO_BASE_URL`. |

### Fallbacks and output

| Field | Type | Default | Notes |
| --- | --- | --- | --- |
| `aiFallback` | bool | `true` | Use getbro's vision model when an endpoint is gated. |
| `aiModelSize` | string | `small` | `small`, `medium`, `large`. |
| `enrichFromEmbed` | bool | `false` | Re-read AI-discovered posts through the embed page (one page load each). |
| `outputFormat` | string | `json` | `json`, `jsonl`, `csv`. |
| `datasetName` | string | `<unique_id>` | Custom dataset directory name. |
| `outputDir` | string | `storage` | Also `$OUTPUT_DIR`. |
| `materializeItems` | bool | `true` | Keep records in `result.items` (Python API); the CLI keeps them only with `--print`. |
| `debug` | bool | `false` | Verbose logging. |

Date filters apply after mapping; a chronological profile feed stops paging
once it passes `onlyPostsNewerThan`. Pinned posts keep their original dates,
so the floor is trusted only after five consecutive older non-pinned posts.

---

## Output reference


### Records

**Posts and reels:** `inputUrl`, `id`, `type` (`Image`/`Video`/`Sidecar`),
`shortCode`, `caption`, `hashtags`, `mentions`, `url`, `commentsCount`,
`firstComment`, `latestComments`, `dimensionsHeight`, `dimensionsWidth`,
`displayUrl`, `images`, `videoUrl`, `alt`, `likesCount`, `videoViewCount`,
`videoPlayCount`, `timestamp`, `childPosts`, `ownerFullName`, `ownerUsername`,
`ownerId`, `productType`, `videoDuration`, `isSponsored`, `isPinned`,
`isCommentsDisabled`, `taggedUsers`, `coauthorProducers`, `musicInfo`,
`locationName`, `locationId`, `dataSource`. `images` holds one URL per media
(one per carousel slide); slides are also expanded into `childPosts`.

**Profile details:** `inputUrl`, `id`, `username`, `url`, `fullName`,
`biography`, `externalUrls`, `externalUrl`, `followersCount`, `followsCount`,
`hasChannel`, `highlightReelCount`, `isBusinessAccount`, `joinedRecently`,
`businessCategoryName`, `private`, `verified`, `profilePicUrl`,
`profilePicUrlHD`, `igtvVideoCount`, `postsCount`, `fbid`, `latestPosts`,
`relatedProfiles`, `dataSource`; with `addProfileStatistics` also
`accountType`, `categoryName`, `businessEmail`, `businessPhoneNumber`,
`businessAddress`, `pronouns`, `hasClips`, `hasGuides` and more.

**Comments:** `inputUrl`, `id`, `postUrl`, `commentUrl`, `text`,
`ownerUsername`, `ownerProfilePicUrl`, `timestamp`, `repliesCount`, `replies`,
`likesCount`, `owner`, `dataSource`; replies (with `includeNestedComments`)
carry `parentCommentId`.

**Hashtags:** `id`, `name`, `url`, `postsCount`, `topPosts`, `latestPosts`.
**Places:** `id`, `name`, `url`, `slug`, `postsCount`, `lat`, `lng`,
`address`, `city`, `phone`, `website`. **Stories:** `id`, `type`, `position`,
`ownerUsername`, `timestamp`, `expiringAt`, `displayUrl`, `videoUrl`,
`mentions`, `hashtags`, `links`.

A count Instagram did not give is `null`, never `0`; a genuine zero stays `0`.
CSV flattens lists of scalars to comma-separated strings and nested objects to
JSON.

### Run summary (`OUTPUT.json`)

| Key | What it holds |
| --- | --- |
| `itemCount`, `status`, `stopReason` | Records written; `succeeded` or `partial`; what stopped the run. |
| `searchHits`, `unsupportedInputs` | Search hits that became targets; accepted Apify inputs this scraper does not honour. |
| `stats` | Targets done / partial / failed, errors by type, failures with messages, fallbacks, comment pagination reports. |
| `billing` | getbro total and one entry per browser (`restarts` counts session swaps), each with `confirmedStopped`. |
| `budget` | Managed reads (initial, retries, enrichment), elapsed seconds, `ipRotations` / `maxIpRotations`. |
| `preflight` | Per browser: route check, cookie injection, `authentication`, `webConfirmed`, `landingPath` when the web app did not appear. |
| `traffic`, `metrics` | Managed vs. other browser requests seen in HAR; new records, time to the first one, getbro command diagnostics (recovered, unknown, stalled dispatches). |
| `lanes`, `targetsNotScraped` | Several browsers: each browser's report; targets no browser got to. |
| `coverage`, `deepCollection`, `commandRecovery`, `responseSource`, `shares` | Complete mode: traversal state, comment counters, the command journal, where comment pages came from, per-account shares. |

---

