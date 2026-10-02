"""Command-line interface.

Two ways to drive the scraper:

* Apify style -- point it at an input file::

      python -m instagram_scraper --input examples/profile_posts.json

* flag style -- everything on the command line::

      python -m instagram_scraper --url instagram.com/nasa --type posts --limit 50

Flags always win over the file, so a stored input can be tweaked per run.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path
from typing import Any

from .errors import FatalError, ScraperError, InputError
from .input_model import build_input, load_input_file
from .runner import ScraperRun
from .storage import RunStorage

log = logging.getLogger("instagram_scraper")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="instagram-scraper",
        description="Scrape Instagram posts, reels, comments, profiles, "
                    "hashtags, places and stories through getbro.ws.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "examples:\n"
            "  instagram-scraper --url instagram.com/nasa --type posts --limit 30\n"
            "  instagram-scraper --url instagram.com/p/DcOX3hWFiey/ --type comments\n"
            "  instagram-scraper --search nasa --search-type user --type details\n"
            "  instagram-scraper --input examples/hashtag_posts.json --format csv\n"
        ),
    )

    source = parser.add_argument_group("what to scrape")
    source.add_argument("--input", "-i", metavar="FILE",
                        help="actor input JSON (Apify INPUT.json format)")
    source.add_argument("--url", "-u", action="append", default=[], metavar="URL",
                        help="Instagram URL, @handle or #hashtag (repeatable)")
    source.add_argument("--type", "-t", dest="results_type",
                        choices=["posts", "reels", "comments", "details",
                                 "mentions", "stories"],
                        help="what to extract (default: posts)")
    source.add_argument("--limit", "-n", type=int, metavar="N",
                        help="max results per URL (default: 200)")
    source.add_argument("--search", "-s", metavar="QUERY",
                        help="comma-separated search queries")
    source.add_argument("--search-type", choices=["hashtag", "user", "place"],
                        help="what a search query resolves to (default: hashtag)")
    source.add_argument("--search-limit", type=int, metavar="N",
                        help="max search hits per query (default: 10)")

    filters = parser.add_argument_group("filters")
    filters.add_argument("--newer-than", metavar="DATE",
                         help="only posts after this date ('3 days', '2025-01-31')")
    filters.add_argument("--until", metavar="DATE", help="only posts before this date")
    filters.add_argument("--skip-pinned", action="store_true",
                         help="drop pinned posts")
    filters.add_argument("--newest-comments", action="store_true",
                         help="return comments newest first")
    filters.add_argument("--nested-comments", action="store_true",
                         help="include comment replies as separate results")
    filters.add_argument("--parent-data", action="store_true",
                         help="tag each post with the profile/hashtag it came from")
    filters.add_argument("--profile-statistics", action="store_true",
                         help="include extended profile metadata in `details`")

    auth = parser.add_argument_group("session")
    auth.add_argument("--cookies", metavar="VALUE", action="append",
                      help="sessionid, 'a=b; c=d' cookie header, or a JSON array; repeat "
                           "for several accounts (one browser each)")
    auth.add_argument("--cookies-file", metavar="FILE", action="append",
                      help="path to a JSON file with an array of cookie objects; repeat "
                           "for several accounts (one browser each)")
    auth.add_argument("--no-ai", action="store_true",
                      help="disable the vision fallback for gated endpoints")
    auth.add_argument("--ai-model", choices=["small", "medium", "large"],
                      help="model size for the AI fallback (default: small)")
    auth.add_argument("--allow-degraded", action="store_true",
                      help="continue with slow AI-scraped results when the "
                           "supplied cookies are rejected, instead of stopping")
    auth.add_argument("--enrich", action="store_true",
                      help="re-read AI-discovered posts through the embed page "
                           "to fill in caption, author and media URL")

    net = parser.add_argument_group("getbro")
    net.add_argument("--api-key", metavar="KEY",
                     help="getbro API key (default: $BRO_API_KEY)")
    net.add_argument("--proxy-tier", choices=["lite", "basic", "premium"],
                     help="proxy quality (default: basic)")
    net.add_argument("--proxy-policy",
                     choices=["html_only", "basic", "extended", "full"],
                     help="what traffic to route through the proxy (default: extended)")
    net.add_argument("--country", metavar="CC",
                     help="exit country for anonymous runs (default: getbro's, UK); runs with "
                          "cookies are pinned to DE/Frankfurt")
    net.add_argument("--concurrency", type=int, metavar="N",
                     help="anonymous browser sessions at once (default: 1); with cookies "
                          "there is one browser per account instead")
    net.add_argument("--max-ip-rotations", type=int, metavar="N",
                     help="bro session swaps an anonymous run may make when Instagram puts an "
                          "exit IP behind its login wall (default: 10; 0 disables; "
                          "runs with cookies never swap)")
    net.add_argument("--bro-recovery-timeout", type=float, metavar="SECONDS",
                     help="budget for resolving one uncertain getbro command (default: 120)")
    net.add_argument("--bro-dispatch-timeout", type=float, metavar="SECONDS",
                     help="seconds a command may stay queued undelivered before that "
                          "is logged as a slow dispatch (diagnostic; default: 45)")

    out = parser.add_argument_group("output")
    out.add_argument("--dataset-name", "--dataset-id", "--run-id", dest="dataset_name",
                     default=None, metavar="NAME",
                     help="output dataset/store directory name (default: unique timestamp slug)")
    out.add_argument("--output-dir", "-o", metavar="DIR",
                     help="storage directory (default: storage)")
    out.add_argument("--format", "-f", dest="output_format",
                     choices=["json", "jsonl", "csv"],
                     help="dataset format (default: json)")
    out.add_argument("--print", dest="print_items", action="store_true",
                     help="also print the records to stdout")
    out.add_argument("--quiet", "-q", action="store_true", help="errors only")
    out.add_argument("--debug", "-d", action="store_true", help="verbose logging")
    out.add_argument("--resume", metavar="DIRECTORY", help="continue a complete-mode checkpoint")
    out.add_argument("--collection-phase", choices=["all", "collect", "enrich"],
                     help="collect then enrich, collect only, or enrich a saved checkpoint only")
    out.add_argument("--expand-owners", action=argparse.BooleanOptionalAction, default=None,
                     help="enable or disable owner profile enrichment (complete mode)")
    out.add_argument("--max-enrichment-requests", type=int, metavar="N",
                     help="maximum enrichment API reads, including retries (0 disables)")
    out.add_argument("--reuse-post-document", action=argparse.BooleanOptionalAction,
                     default=None,
                     help="complete chronological comments: read every post from one "
                          "opened post document (default) or open each post's page")
    out.add_argument("--revisit-ranked-parents-on-resume",
                     action=argparse.BooleanOptionalAction, default=None,
                     help="on explicit resume, start another ranked parent-list pass")

    return parser


def merge_args(args: argparse.Namespace) -> dict[str, Any]:
    """Overlay CLI flags on top of an optional input file."""
    raw: dict[str, Any] = {}
    if args.input:
        raw = load_input_file(args.input)
    if args.resume:
        from .state import StateStore
        checkpoint = Path(args.resume) / "state.sqlite"
        if not checkpoint.is_file():
            raise InputError("Resume directory has no state.sqlite")
        saved = StateStore(checkpoint)
        previous = saved.get_meta("input", {})
        saved.close()
        previous.pop("session_cookies", None)
        previous.pop("extra_accounts", None)
        bro = previous.pop("bro", {})
        for field, key in (("base_url", "broBaseUrl"), ("session_timeout", "broSessionTimeout"),
                           ("command_timeout", "broCommandTimeout"), ("recovery_timeout", "broRecoveryTimeout"),
                           ("skip_balance_check", "broSkipBalanceCheck"),
                           # ``transport`` of pre-SDK checkpoints is dropped here
                           ("dispatch_timeout", "broDispatchTimeout"),
                           ("read_timeout", "broReadTimeout")):
            if field in bro:
                previous[key] = bro[field]
        proxy = previous.pop("proxy", {})
        previous.update({"proxyCountry": proxy.get("country"), "proxyCity": proxy.get("city"),
                         "proxyPolicy": proxy.get("policy", "full"), "proxyTier": proxy.get("tier", "basic")})
        raw = {**previous, **raw, "resume": args.resume, "collectionMode": "complete", "outputDir": args.resume}

    if args.url:
        raw["directUrls"] = list(raw.get("directUrls") or []) + list(args.url)
    for flag, key in (
        ("results_type", "resultsType"), ("limit", "resultsLimit"),
        ("collection_phase", "collectionPhase"), ("max_enrichment_requests", "maxEnrichmentRequests"),
        ("expand_owners", "expandOwners"),
        ("revisit_ranked_parents_on_resume", "revisitRankedParentsOnResume"),
        ("reuse_post_document", "reusePostDocument"),
        ("search", "search"), ("search_type", "searchType"),
        ("search_limit", "searchLimit"), ("newer_than", "onlyPostsNewerThan"),
        ("until", "untilDate"), ("ai_model", "aiModelSize"),
        ("api_key", "broApiKey"), ("proxy_tier", "proxyTier"),
        ("proxy_policy", "proxyPolicy"), ("country", "proxyCountry"),
        ("concurrency", "broConcurrency"), ("dataset_name", "datasetName"), ("output_dir", "outputDir"),
        ("output_format", "outputFormat"), ("max_ip_rotations", "maxIpRotations"),
        ("bro_recovery_timeout", "broRecoveryTimeout"),
        ("bro_dispatch_timeout", "broDispatchTimeout"),
    ):
        value = getattr(args, flag, None)
        if value is not None:
            raw[key] = value

    for flag, key in (
        ("skip_pinned", "skipPinnedPosts"), ("newest_comments", "isNewestComments"),
        ("nested_comments", "includeNestedComments"), ("parent_data", "addParentData"),
        ("profile_statistics", "addProfileStatistics"), ("debug", "debug"),
        ("enrich", "enrichFromEmbed"),
    ):
        if getattr(args, flag, False):
            raw[key] = True

    if args.allow_degraded:
        raw["requireValidSession"] = False
    if args.no_ai:
        raw["aiFallback"] = False

    # Each --cookies-file / --cookies is one account; flags replace the
    # accounts of an input file.
    accounts: list[Any] = [json.loads(Path(path).read_text(encoding="utf-8"))
                           for path in args.cookies_file or []]
    accounts += list(args.cookies or [])
    if accounts:
        raw["sessionCookies"] = accounts[0]
        raw.pop("sessionCookiesList", None)
        raw.pop("session_cookies_list", None)
        if len(accounts) > 1:
            raw["sessionCookiesList"] = accounts[1:]

    return raw


class _BrowserTag(logging.Filter):
    """Prefix a parallel run's lines with their browser (``[account-2]``)."""

    def filter(self, record: logging.LogRecord) -> bool:
        name = record.threadName or ""
        tag = f"[{name}] " if name.startswith(("account-", "worker-")) else ""
        # a browser's own session lines already start with its label
        record.browser = "" if tag and str(record.msg).startswith("[%s]") and record.args \
            and record.args[0] == name else tag
        return True


def setup_logging(*, debug: bool = False, quiet: bool = False) -> None:
    level = logging.ERROR if quiet else (logging.DEBUG if debug else logging.INFO)
    root = logging.getLogger()
    configured = bool(root.handlers)
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)-7s %(name)s: %(browser)s%(message)s",
        datefmt="%H:%M:%S",
        stream=sys.stderr,
    )
    if not configured:
        for handler in root.handlers:
            handler.addFilter(_BrowserTag())
    # The transport logs every retry at DEBUG; keep it quiet unless asked.
    if not debug:
        logging.getLogger("instagram_scraper.bro.client").setLevel(logging.INFO)
        logging.getLogger("instagram_scraper.bro.sdk_client").setLevel(logging.INFO)


def load_dotenv(path: str | Path = ".env") -> None:
    """Minimal .env loader so `BRO_API_KEY` can live next to the project."""
    file = Path(path)
    if not file.exists():
        return
    for line in file.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    setup_logging(debug=args.debug, quiet=args.quiet)

    load_dotenv()
    load_dotenv(Path(__file__).resolve().parents[2] / ".env")

    try:
        raw = merge_args(args)
        config = build_input(raw)
    except ScraperError as exc:
        parser.error(str(exc))
        return 2
    except (OSError, json.JSONDecodeError) as exc:
        print(f"error: cannot read input: {exc}", file=sys.stderr)
        return 2

    from .state import PersistentStorage
    config.materialize_items = args.print_items
    try:
        storage = (PersistentStorage(config.resume or config.output_dir, fmt=config.output_format,
                                     resume=bool(config.resume), name=config.dataset_name) if config.collection_mode == "complete"
                   else RunStorage(config.output_dir, fmt=config.output_format, name=config.dataset_name))
    except ScraperError as exc:
        log.error("%s", exc)
        return 2
    log.info("resultsType=%s limit=%d ai_fallback=%s auth=%s",
             config.results_type.value, config.results_limit, config.ai_fallback,
             f"{len(config.account_jars())} accounts" if len(config.account_jars()) > 1
             else bool(config.session_cookies))

    try:
        result = ScraperRun(config, storage=storage).run()
    except FatalError as exc:
        log.error("%s", exc)
        return 3
    except KeyboardInterrupt:
        log.warning("interrupted; flushing what was collected so far")
        storage.finish()
        return 130
    except ScraperError as exc:
        log.error("run failed: %s", exc)
        return 1

    if args.print_items:
        json.dump(result.items, sys.stdout, ensure_ascii=False, indent=2, default=str)
        sys.stdout.write("\n")

    _print_summary(result)
    return 0 if result.stats.items or not result.stats.targets_failed else 4


def _print_summary(result: Any) -> None:
    stats = result.stats
    lines = [
        "",
        f"  items          {stats.items}",
        f"  targets ok     {stats.targets_done}",
        f"  targets failed {stats.targets_failed}",
        f"  targets partial {stats.targets_partial}",
        f"  getbro cost    ${result.billing.get('total', 0.0):.4f}",
    ]
    if stats.fallbacks:
        lines.append(f"  fallbacks      {dict(stats.fallbacks)}")
    for lane in getattr(result, "lanes", None) or []:
        detail = (f"{lane.get('records', lane.get('itemCount'))} record(s)"
                  if "records" in lane or "itemCount" in lane else "")
        reason = lane.get("reason") or lane.get("error") or lane.get("stopReason")
        lines.append(f"  {lane.get('label', '?'):<14} {lane.get('status')}"
                     + (f", {detail}" if detail else "") + (f" ({reason})" if reason else ""))
    if result.dataset_path:
        lines.append(f"  dataset        {result.dataset_path}")
    if stats.failures:
        lines.append("  failures:")
        for failure in stats.failures[:10]:
            lines.append(f"    - {failure['target']}: {failure['error']}: "
                         f"{failure['message'][:120]}")
    print("\n".join(lines), file=sys.stderr)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
