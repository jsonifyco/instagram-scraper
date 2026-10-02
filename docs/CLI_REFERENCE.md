## CLI reference


```
what to scrape
  --input, -i FILE        Apify-style INPUT.json
  --url, -u URL           Instagram URL, @handle or #hashtag (repeatable)
  --type, -t TYPE         posts | reels | comments | details | mentions | stories
  --limit, -n N           results per URL (default 200)
  --search, -s QUERY      comma-separated search queries
  --search-type TYPE      hashtag | user | place
  --search-limit N        hits per query (default 10)

filters
  --newer-than DATE       "3 days", "2026-01-31", ISO timestamp
  --until DATE            upper bound
  --skip-pinned           drop pinned posts
  --newest-comments       comments newest first (complete mode: chronological model)
  --nested-comments       replies as separate records
  --parent-data           tag posts with their source profile/hashtag/place
  --profile-statistics    extended profile metadata

session
  --cookies VALUE         sessionid, cookie header or JSON array (repeat: one per account)
  --cookies-file FILE     JSON file with cookie objects (repeat: one per account)
  --no-ai                 disable the vision fallback
  --ai-model SIZE         small | medium | large
  --allow-degraded        keep going when the cookies are rejected
  --enrich                re-read AI-discovered posts through the embed page

getbro
  --api-key KEY           getbro key (default $BRO_API_KEY)
  --proxy-tier TIER       lite | basic | premium
  --proxy-policy POLICY   html_only | basic | extended | full
  --country CC            exit country for anonymous runs (default: getbro's, UK)
  --concurrency N         anonymous browsers at once (with cookies: one per account)
  --max-ip-rotations N    session swaps behind a login wall, anonymous runs (default 10)
  --bro-recovery-timeout SECONDS
  --bro-dispatch-timeout SECONDS

output
  --output-dir, -o DIR    storage directory (default storage)
  --format, -f FORMAT     json | jsonl | csv
  --print                 also print records to stdout
  --quiet, -q / --debug, -d
  --resume DIRECTORY      continue a complete-mode checkpoint
  --collection-phase MODE all | collect | enrich
  --expand-owners / --no-expand-owners
  --max-enrichment-requests N
  --reuse-post-document / --no-reuse-post-document
  --revisit-ranked-parents-on-resume / --no-revisit-ranked-parents-on-resume
```

Flags win over the input file. Each `--cookies` / `--cookies-file` is one
account and replaces the input file's accounts. `--resume` restores the saved
non-secret input; cookies must be passed again (an attached live session keeps its
jar and is not injected twice).

Exit codes: `0` success, `1` run failed, `2` bad input, `3` fatal (bad key, no
balance, an account refused or challenged, a login lost mid-run), `4` finished
with no items and at least one failed target, `130` interrupted (partial
output is still written).

---

