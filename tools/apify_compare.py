"""Run apify/instagram-scraper on the same targets and record what it actually costs.

    python tools/apify_compare.py --output-dir verification/apify-comparison-2026-09-10
    python tools/apify_compare.py --input probe.json --output-dir verification/apify-<name>

Only an explicit invocation spends Apify credit. The script stores the run
metadata, the billed amount reported by Apify and the dataset it produced.
``--input`` sends an arbitrary actor INPUT (any resultsType, search, date
filters) instead of the default comments payload.
"""
from __future__ import annotations
import argparse
import json
import os
from pathlib import Path
import sys
import time
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from instagram_scraper.cli import load_dotenv  # noqa: E402

BASE = "https://api.apify.com/v2"
ACTOR = "apify~instagram-scraper"


def call(url, token, *, payload=None, method=None):
    sep = "&" if "?" in url else "?"
    request = urllib.request.Request(
        f"{url}{sep}token={token}",
        data=json.dumps(payload).encode() if payload is not None else None,
        headers={"Content-Type": "application/json"},
        method=method or ("POST" if payload is not None else "GET"),
    )
    with urllib.request.urlopen(request, timeout=120) as response:
        body = response.read().decode("utf-8")
    return json.loads(body) if body else {}


def billed(run):
    """Apify reports several cost fields; keep every one we can see."""
    fields = {k: run.get(k) for k in (
        "usageTotalUsd", "chargedEventCounts", "pricingInfo",
        "generalAccess", "usageUsd") if run.get(k) is not None}
    stats = run.get("stats") or {}
    for k in ("computeUnits", "durationMillis"):
        if k in stats:
            fields[k] = stats[k]
    return fields


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--urls", type=Path, help="JSON list of post URLs")
    parser.add_argument("--input", type=Path, help="full actor INPUT JSON (overrides --urls/--results-limit)")
    parser.add_argument("--results-limit", type=int, default=100)
    parser.add_argument("--poll", type=float, default=15.0)
    parser.add_argument("--max-wait", type=float, default=3600.0)
    args = parser.parse_args()

    load_dotenv(ROOT / ".env")
    token = os.environ.get("APIFY_API_KEY") or os.environ.get("APIFY_TOKEN")
    if not token:
        raise SystemExit("APIFY_API_KEY is not set")

    if args.input:
        payload = json.loads(args.input.read_text(encoding="utf-8"))
        urls = list(payload.get("directUrls") or [])
    else:
        if args.urls:
            urls = json.loads(args.urls.read_text(encoding="utf-8"))
        else:
            profile = json.loads((ROOT / "examples/comments_complete_12_posts.json").read_text(encoding="utf-8"))
            urls = profile["directUrls"]
        payload = {"directUrls": urls, "resultsType": "comments",
                   "resultsLimit": args.results_limit, "addParentData": False}
    args.output_dir.mkdir(parents=True, exist_ok=True)
    (args.output_dir / "INPUT.json").write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")

    started = time.monotonic()
    run = call(f"{BASE}/acts/{ACTOR}/runs", token, payload=payload)["data"]
    run_id = run["id"]
    print(json.dumps({"event": "started", "runId": run_id, "urls": len(urls),
                      "resultsLimit": args.results_limit}, ensure_ascii=False), flush=True)

    while run.get("status") in ("READY", "RUNNING"):
        if time.monotonic() - started > args.max_wait:
            print(json.dumps({"event": "timeout", "runId": run_id}), flush=True)
            break
        time.sleep(args.poll)
        run = call(f"{BASE}/actor-runs/{run_id}", token)["data"]
        print(json.dumps({"event": "poll", "status": run.get("status"),
                          "seconds": round(time.monotonic() - started, 1)}), flush=True)

    items, offset = [], 0
    dataset = run.get("defaultDatasetId")
    while dataset:
        page = call(f"{BASE}/datasets/{dataset}/items?offset={offset}&limit=1000", token)
        if not page:
            break
        items.extend(page)
        offset += len(page)
        if len(page) < 1000:
            break

    (args.output_dir / "items.json").write_text(
        json.dumps(items, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
    summary = {"runId": run_id, "status": run.get("status"),
               "startedAt": run.get("startedAt"), "finishedAt": run.get("finishedAt"),
               "seconds": round(time.monotonic() - started, 1),
               "items": len(items), "billing": billed(run), "urls": len(urls),
               "resultsLimit": args.results_limit}
    (args.output_dir / "RUN.json").write_text(
        json.dumps({"summary": summary, "run": run}, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"event": "finished", **summary}, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main()
