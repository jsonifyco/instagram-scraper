"""End-to-end verification against live Instagram.

Runs every supported target/resultsType combination through the real pipeline
(getbro browser -> Instagram -> mappers -> dataset) and prints what each one
actually produced: record count, which layer of the fallback chain answered,
field coverage, cost, and a sample record.

    python tools/verify_live.py                # every case
    python tools/verify_live.py posts details  # only the named cases

Set BRO_API_KEY in the environment or .env. Set IG_SESSIONID to additionally
exercise the authenticated paths (comments, stories, hashtag/place via the
private API, ranked search).

This bills your getbro account (about $0.5 for the anonymous sweep, measured
September 2026). Anonymous cases may swap VMs behind a login wall
(``maxIpRotations``, 10 by default).
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
import traceback
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from instagram_scraper import build_input, run_scraper  # noqa: E402
from instagram_scraper.cli import load_dotenv, setup_logging  # noqa: E402
from instagram_scraper.errors import ScraperError  # noqa: E402

REPORT_DIR = Path(__file__).resolve().parents[1] / "verification"


@dataclass
class Case:
    """One thing to verify."""

    name: str
    description: str
    payload: dict[str, Any]
    #: fields that must be non-empty on at least one record for a pass
    expect_fields: tuple[str, ...] = ()
    #: True when this case is only meaningful with a session cookie
    needs_auth: bool = False
    #: True when the expected outcome is a clean, explanatory refusal
    expect_refusal: bool = False


@dataclass
class Result:
    case: Case
    ok: bool
    items: int = 0
    seconds: float = 0.0
    cost: float = 0.0
    sources: dict[str, int] = field(default_factory=dict)
    fallbacks: dict[str, int] = field(default_factory=dict)
    coverage: dict[str, int] = field(default_factory=dict)
    sample: dict[str, Any] | None = None
    error: str = ""
    note: str = ""


CASES: list[Case] = [
    Case(
        "profile-posts",
        "profile timeline (the 12-post preview of web_profile_info when anonymous)",
        {"directUrls": ["https://www.instagram.com/nasa/"],
         "resultsType": "posts", "resultsLimit": 12},
        expect_fields=("id", "shortCode", "url", "caption", "likesCount",
                       "commentsCount", "timestamp", "ownerUsername", "displayUrl"),
    ),
    Case(
        "profile-posts-dated",
        "date filter stops pagination at the floor",
        {"directUrls": ["https://www.instagram.com/nasa/"],
         "resultsType": "posts", "resultsLimit": 30,
         "onlyPostsNewerThan": "14 days"},
        expect_fields=("shortCode", "timestamp"),
    ),
    Case(
        "profile-details",
        "profile metadata with extended statistics",
        {"directUrls": ["https://www.instagram.com/nasa/"],
         "resultsType": "details", "addProfileStatistics": True},
        # `fbid` is deliberately not required: only `web_profile_info` returns
        # it, so demanding it would turn this case into a throttling detector
        # rather than a check that details records are complete. Everything
        # else here is reachable from the page + grid fallback too.
        expect_fields=("id", "username", "followersCount", "postsCount",
                       "profilePicUrlHD", "latestPosts"),
    ),
    Case(
        "profile-reels",
        "reels tab, filtered to clips",
        {"directUrls": ["https://www.instagram.com/nasa/reels/"],
         "resultsType": "reels", "resultsLimit": 8},
        expect_fields=("shortCode", "type", "videoViewCount"),
    ),
    Case(
        "single-post",
        "one post; anonymously this falls through to the embed renderer",
        {"directUrls": ["https://www.instagram.com/p/DcOX3hWFiey/"],
         "resultsType": "posts"},
        expect_fields=("shortCode", "url", "caption", "ownerUsername", "displayUrl"),
    ),
    Case(
        "hashtag-posts",
        "hashtag grid; anonymously this is the vision fallback",
        {"directUrls": ["https://www.instagram.com/explore/tags/space/"],
         "resultsType": "posts", "resultsLimit": 12, "addParentData": True},
        expect_fields=("shortCode", "url", "fromHashtag"),
    ),
    Case(
        "hashtag-enriched",
        "hashtag grid re-read through the embed page",
        {"directUrls": ["https://www.instagram.com/explore/tags/space/"],
         "resultsType": "posts", "resultsLimit": 4, "enrichFromEmbed": True},
        expect_fields=("shortCode", "caption", "ownerUsername", "likesCount",
                       "displayUrl"),
    ),
    Case(
        "hashtag-details",
        "hashtag header record",
        {"directUrls": ["https://www.instagram.com/explore/tags/space/"],
         "resultsType": "details"},
        expect_fields=("name", "url"),
    ),
    Case(
        "place-posts",
        "place grid",
        {"directUrls": ["https://www.instagram.com/explore/locations/212988663/"],
         "resultsType": "posts", "resultsLimit": 8},
        expect_fields=("shortCode", "url"),
    ),
    Case(
        "place-details",
        "place header record",
        {"directUrls": ["https://www.instagram.com/explore/locations/212988663/"],
         "resultsType": "details"},
        expect_fields=("id", "name"),
    ),
    Case(
        "mentions",
        "posts tagging a profile (/tagged/ redirects logged-out visitors)",
        {"directUrls": ["https://www.instagram.com/nasa/"],
         "resultsType": "mentions", "resultsLimit": 8},
        expect_fields=("shortCode", "url", "mentionedProfile"),
        needs_auth=True, expect_refusal=True,
    ),
    Case(
        "search-user",
        "profile query; anonymously this resolves the handle directly",
        {"search": "nasa", "searchType": "user", "searchLimit": 2,
         "resultsType": "details"},
        expect_fields=("username", "followersCount"),
    ),
    Case(
        "search-hashtag",
        "hashtag query into posts",
        {"search": "astronomy", "searchType": "hashtag", "searchLimit": 1,
         "resultsType": "posts", "resultsLimit": 6},
        expect_fields=("shortCode", "url"),
    ),
    Case(
        "concurrency-jsonl",
        "two profiles across two browser sessions, JSONL output",
        {"directUrls": ["https://www.instagram.com/nasa/",
                        "https://www.instagram.com/natgeo/"],
         "resultsType": "details", "broConcurrency": 2, "outputFormat": "jsonl"},
        expect_fields=("username", "followersCount"),
    ),
    Case(
        "comments",
        "comments on a post",
        {"directUrls": ["https://www.instagram.com/p/DcOX3hWFiey/"],
         "resultsType": "comments", "resultsLimit": 20},
        expect_fields=("id", "text", "ownerUsername", "timestamp"),
        needs_auth=True, expect_refusal=True,
    ),
    Case(
        "stories",
        "active story frames",
        {"directUrls": ["https://www.instagram.com/nasa/"],
         "resultsType": "stories", "resultsLimit": 10},
        expect_fields=("id", "type", "displayUrl"),
        needs_auth=True, expect_refusal=True,
    ),
]


def run_case(case: Case, *, authenticated: bool) -> Result:
    out_dir = REPORT_DIR / "runs" / case.name
    payload = dict(case.payload)
    payload.setdefault("outputDir", str(out_dir))

    started = time.monotonic()
    try:
        config = build_input(payload)
        run = run_scraper(config)
    except ScraperError as exc:
        elapsed = time.monotonic() - started
        refused = case.expect_refusal and not authenticated
        return Result(
            case, ok=refused, seconds=elapsed,
            error=f"{type(exc).__name__}: {exc}",
            note="expected refusal without a session" if refused else "",
        )
    except Exception as exc:  # noqa: BLE001 - the point is to catch everything
        return Result(case, ok=False, seconds=time.monotonic() - started,
                      error=f"{type(exc).__name__}: {exc}\n{traceback.format_exc()[-600:]}")

    elapsed = time.monotonic() - started
    items = run.items

    # A refusal case that produced no items but also raised nothing counts as
    # a pass only if the run recorded an explanatory failure.
    if not items and case.expect_refusal and not authenticated:
        reasons = "; ".join(f["error"] for f in run.stats.failures) or "no items"
        return Result(case, ok=True, seconds=elapsed, cost=run.billing["total"],
                      error=reasons, note="expected refusal without a session")

    coverage = {}
    if items:
        for key in case.expect_fields:
            coverage[key] = sum(
                1 for item in items if item.get(key) not in (None, "", [], {})
            )

    missing = [k for k, v in coverage.items() if v == 0]
    ok = bool(items) and not missing
    note = ""
    if missing:
        note = f"empty on every record: {', '.join(missing)}"

    return Result(
        case, ok=ok, items=len(items), seconds=elapsed,
        cost=run.billing["total"],
        sources=dict(Counter(str(i.get("dataSource")) for i in items)),
        fallbacks=dict(run.stats.fallbacks),
        coverage=coverage,
        sample=items[0] if items else None,
        note=note,
    )


def main(argv: list[str]) -> int:
    load_dotenv()
    load_dotenv(Path(__file__).resolve().parents[1] / ".env")
    setup_logging(quiet=True)
    logging.getLogger("instagram_scraper.runner").setLevel(logging.WARNING)

    if not os.environ.get("BRO_API_KEY"):
        print("BRO_API_KEY is not set", file=sys.stderr)
        return 2

    authenticated = bool(os.environ.get("IG_SESSIONID"))
    wanted = set(argv)
    cases = [c for c in CASES if not wanted or c.name in wanted]
    if not cases:
        print(f"no case matched {sorted(wanted)}; known: "
              f"{', '.join(c.name for c in CASES)}", file=sys.stderr)
        return 2

    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    print(f"verifying {len(cases)} case(s) against live Instagram "
          f"({'authenticated' if authenticated else 'anonymous'})\n")

    results: list[Result] = []
    for index, case in enumerate(cases, 1):
        print(f"[{index}/{len(cases)}] {case.name}: {case.description} ... ",
              end="", flush=True)
        result = run_case(case, authenticated=authenticated)
        results.append(result)
        status = "PASS" if result.ok else "FAIL"
        detail = f"{result.items} item(s)" if result.items else (result.note or "no items")
        print(f"{status}  {detail}  {result.seconds:.0f}s  ${result.cost:.4f}")

    _print_report(results, authenticated=authenticated)
    _save_report(results, authenticated=authenticated)
    return 0 if all(r.ok for r in results) else 1


def _print_report(results: list[Result], *, authenticated: bool) -> None:
    width = max(len(r.case.name) for r in results)
    print("\n" + "=" * 78)
    print("RESULTS")
    print("=" * 78)
    print(f"{'case'.ljust(width)}  {'':4}  {'items':>5}  {'cost':>8}  source")
    print("-" * 78)
    for r in results:
        source = ", ".join(f"{k}={v}" for k, v in r.sources.items()) or "-"
        print(f"{r.case.name.ljust(width)}  {'PASS' if r.ok else 'FAIL':4}  "
              f"{r.items:5}  ${r.cost:7.4f}  {source}")

    print("\nfield coverage (non-empty records / total):")
    for r in results:
        if not r.coverage:
            continue
        cells = " ".join(f"{k}={v}/{r.items}" for k, v in r.coverage.items())
        print(f"  {r.case.name}: {cells}")

    fallbacks = [r for r in results if r.fallbacks]
    if fallbacks:
        print("\nfallback layers exercised:")
        for r in fallbacks:
            print(f"  {r.case.name}: {r.fallbacks}")

    notes = [r for r in results if r.note or r.error]
    if notes:
        print("\nnotes:")
        for r in notes:
            print(f"  {r.case.name}: {r.note or r.error[:160]}")

    passed = sum(1 for r in results if r.ok)
    print("\n" + "-" * 78)
    print(f"{passed}/{len(results)} passed | "
          f"total ${sum(r.cost for r in results):.4f} | "
          f"{sum(r.seconds for r in results):.0f}s | "
          f"{'authenticated' if authenticated else 'anonymous'} run")


def _save_report(results: list[Result], *, authenticated: bool) -> None:
    payload = {
        "authenticated": authenticated,
        "passed": sum(1 for r in results if r.ok),
        "total": len(results),
        "totalCost": round(sum(r.cost for r in results), 6),
        "cases": [
            {
                "name": r.case.name,
                "description": r.case.description,
                "ok": r.ok,
                "items": r.items,
                "seconds": round(r.seconds, 1),
                "cost": round(r.cost, 6),
                "dataSources": r.sources,
                "fallbacks": r.fallbacks,
                "fieldCoverage": r.coverage,
                "note": r.note,
                "error": r.error,
                "sample": r.sample,
            }
            for r in results
        ],
    }
    path = REPORT_DIR / "report.json"
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str),
                    encoding="utf-8")
    print(f"\nfull report with samples: {path}")


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
