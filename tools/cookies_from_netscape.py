"""Convert a Netscape cookie file into the JSON array the scraper accepts.

Browser extensions (Cookie-Editor, EditThisCookie, `curl -c`) export the
Netscape format:

    #HttpOnly_.instagram.com  TRUE  /  TRUE  1820047188  sessionid  11913...

`sessionCookies` wants `[{"name": ..., "value": ..., "domain": ..., "path": ...}]`,
so this rewrites one into the other.

    python tools/cookies_from_netscape.py cookies.txt -o .cookies.json
    python tools/cookies_from_netscape.py cookies.txt | \
        python -m instagram_scraper --url @nasa --cookies-file /dev/stdin

The output contains live credentials: write it somewhere gitignored, and treat
it like a password.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

#: cookies Instagram actually needs; everything else is noise but harmless
ESSENTIAL = ("sessionid", "ds_user_id", "csrftoken")
USEFUL = ESSENTIAL + ("mid", "ig_did", "datr", "rur", "shbid", "shbts")


def parse_netscape(text: str) -> list[dict[str, Any]]:
    """Parse a Netscape cookie file into cookie dicts."""
    cookies: list[dict[str, Any]] = []
    for raw in text.splitlines():
        line = raw.strip()
        if not line:
            continue
        # "#HttpOnly_" is a real prefix, not a comment; anything else with a
        # leading # is.
        http_only = False
        if line.startswith("#HttpOnly_"):
            http_only = True
            line = line[len("#HttpOnly_"):]
        elif line.startswith("#"):
            continue

        fields = line.split("\t")
        if len(fields) < 7:
            fields = [f for f in line.split() if f]
        if len(fields) < 7:
            continue

        domain, _flag, path, secure, expires, name, value = fields[:7]
        cookies.append({
            "name": name,
            "value": value,
            "domain": domain,
            "path": path or "/",
            "secure": str(secure).upper() == "TRUE",
            "httpOnly": http_only,
            "expires": int(expires) if str(expires).isdigit() else None,
        })
    return cookies


def summarise(cookies: list[dict[str, Any]]) -> str:
    """A description safe to print: names only, never values."""
    names = [c["name"] for c in cookies]
    missing = [n for n in ESSENTIAL if n not in names]
    lines = [f"{len(cookies)} cookie(s): {', '.join(sorted(names))}"]
    if missing:
        lines.append(f"WARNING: missing essential cookie(s): {', '.join(missing)}")
    if "rd_challenge" in names:
        lines.append(
            "NOTE: an `rd_challenge` cookie is present, which usually means "
            "Instagram has asked this account to complete a checkpoint. "
            "Requests may be refused until it is cleared in a real browser."
        )
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("input", help="Netscape cookie file ('-' for stdin)")
    parser.add_argument("-o", "--output", help="write JSON here (default: stdout)")
    parser.add_argument("--only-essential", action="store_true",
                        help="keep just the cookies Instagram needs")
    args = parser.parse_args(argv)

    text = sys.stdin.read() if args.input == "-" else Path(args.input).read_text("utf-8")
    cookies = parse_netscape(text)
    if not cookies:
        print("no cookies parsed -- is this a Netscape cookie file?", file=sys.stderr)
        return 1
    if args.only_essential:
        cookies = [c for c in cookies if c["name"] in USEFUL]

    print(summarise(cookies), file=sys.stderr)
    payload = json.dumps(cookies, indent=2)
    if args.output:
        path = Path(args.output)
        path.write_text(payload, encoding="utf-8")
        print(f"wrote {path} -- contains live credentials, keep it out of git",
              file=sys.stderr)
    else:
        print(payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
