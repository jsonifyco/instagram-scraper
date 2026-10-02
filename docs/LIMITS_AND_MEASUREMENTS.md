## Cost


getbro bills compute, proxy bandwidth and AI tokens separately; every run
reports its own total in `OUTPUT.billing`, read after each bro session is
confirmed stopped. What drives it:

- **The answering layer.** The API path costs a fraction of the vision
  fallback; with a session, gated endpoints answer directly.
- **Browser time and bandwidth.** Most of an anonymous run's spend. Every
  extra browser and every session swap pays another start-up. `proxyPolicy` and
  `proxyTier` trade bandwidth price against reliability.
- **What Instagram lets through.** The same input can yield very different
  numbers of records on different IPs and hours, so cost per record varies
  far more than cost per browser minute.

`apify/instagram-scraper` charges a flat price per result; a per-browser bill
becomes comparable only once you know how many records a browser yields on
your targets. `tools/apify_compare.py` runs both on the same input.

---

## Testing and tools


```bash
python -m pytest tests -q            # offline suite, no network
python tools/verify_live.py          # live sweep of every target/resultsType; bills getbro
python tools/verify_live.py profile-details single-post
```

The offline suite covers URL classification, shortcodes, input validation and
date formats, the mappers, response classification, auth gates and the
fallback policy, the getbro recovery loops, the SQLite checkpoint and resume,
the comment models, parallel browsers and account shares, and the session swaps.

`tools/` holds the utilities that stay useful:

| Tool | Use |
| --- | --- |
| `verify_live.py` | the live sweep above |
| `cookies_from_netscape.py` | convert a Netscape `cookies.txt` export into the JSON the scraper takes |
| `launch_detached_collection.py` | start a long Windows run outside the caller's process job |
| `summarize_collection_run.py` | read-only audit of a comment run's checkpoint and output |
| `inspect_saved_session.py` | inspect a stopped getbro session without exposing cookies |
| `apify_compare.py` | run `apify/instagram-scraper` on the same input and record its cost |

---

## Known limits

- **One post's comments are read by one browser**; several accounts speed up
  several posts, not one.
- **Anonymous capacity.** An IP can meet the login
  wall on its first read; session swaps and parallel browsers are the way around
  it, and the share of usable IPs changes by the hour.
- **Anonymous posts stop at the profile preview.** The REST feed refuses
  logged-out callers and the logged-out grid query does not paginate.
- **Accounts wear out.** Long comment runs can end with Instagram asking the
  account to log in again, and a new account may get truncated comment lists
  or an identity check. Compare per-account totals before trusting a
  multi-account comment run, and check an account in a browser after such a
  stop: only its owner can pass Instagram's checks.
- **Rejected cookies stop the run** instead of degrading to the vision
  fallback (`requireValidSession`, `--allow-degraded` to opt out). Cookies that
  land on the saved-account screen get one press of "Continue"; a password
  prompt or checkpoint stops the run with a request for fresh cookies.
- **Comment coverage is reported, not assumed.** `source_exhausted` means the
  endpoint stopped advertising pages. Instagram's own comment counters can
  exceed what the web client is served even when every list reached its
  server end.
- **A browser lives at most 6 hours** (`broSessionTimeout`); complete mode
  saves its checkpoint and continues on an explicit `--resume`, never by
  replaying cookies into a new session on its own.
- **Search needs cookies for ranked results**; anonymous `place` queries fail.
- **Feed-sourced posts have no `firstComment`, `latestComments` or `alt`**;
  they fill in when a post is read through `media/info` with a session.
- **The vision fallback returns what the page shows**: a logged-out hashtag
  grid has tiles and view counts only, until `--enrich`.
- Scrape only public data, respect Instagram's terms, and keep request volume
  reasonable.
