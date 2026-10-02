"""Inspect one stopped getbro session without exposing cookies or headers.

This uses the admin read API only.  It never attaches to the browser and never
submits a command, so it is safe to use after an account-related stop.
"""
from __future__ import annotations

import argparse
from collections import Counter
import json
from pathlib import Path
import sys
from typing import Any
from urllib.parse import urlsplit

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import requests

from instagram_scraper.bro.client import USER_AGENT, step_data
from instagram_scraper.cli import load_dotenv
from instagram_scraper.input_model import build_input
from instagram_scraper.ig.network import NetworkCapture, _bodies, _objects


SAFE_ERROR_FIELDS = ("status", "message", "error_type", "require_login")


def safe_location(value: Any) -> dict[str, str]:
    """Return only host/path; query strings may contain request tokens."""
    try:
        parsed = urlsplit(str(value or ""))
    except ValueError:
        return {}
    return {k: v for k, v in {"host": parsed.hostname, "path": parsed.path}.items() if v}


def response_result(data: Any) -> dict[str, Any]:
    if not isinstance(data, dict):
        return {}
    nested = data.get("result")
    return nested if isinstance(nested, dict) else data


def summarize_fetch(data: Any) -> dict[str, Any]:
    result = response_result(data)
    body = result.get("json")
    summary: dict[str, Any] = {
        "httpStatus": result.get("status"),
        "hasText": bool(result.get("text")),
        "hasJson": isinstance(body, (dict, list)),
    }
    text = str(result.get("text") or "")
    lowered = text.lower()
    summary.update({
        "textLength": len(text),
        "looksLikeHtml": text.lstrip().startswith("<"),
        "loginMarker": any(marker in lowered for marker in (
            "login_required", "require_login", "accounts/login", "loginform",
            "log into instagram", "not-logged-in")),
        "rateMarker": "please wait a few minutes" in lowered or "too many requests" in lowered,
        "challengeMarker": "challenge_required" in lowered or "checkpoint_required" in lowered,
    })
    headers = result.get("headers") or {}
    if isinstance(headers, dict):
        content_type = next((value for key, value in headers.items()
                             if str(key).lower() == "content-type"), None)
        if content_type:
            summary["contentType"] = str(content_type).split(";", 1)[0]
    if isinstance(body, dict):
        summary["jsonKeys"] = sorted(str(key) for key in body)[:50]
        error = {key: body.get(key) for key in SAFE_ERROR_FIELDS if key in body}
        if "challenge" in body or "checkpoint_url" in body:
            error["accountAction"] = True
        if error:
            summary["errorMetadata"] = error
        for name in ("comments", "child_comments", "items", "users"):
            if isinstance(body.get(name), list):
                summary[f"{name}Count"] = len(body[name])
        for name in ("next_min_id", "next_max_id", "next_cursor"):
            if body.get(name) is not None:
                summary[f"has_{name}"] = True
    return summary


def summarize_har(data: Any, media_id: str | None = None) -> dict[str, Any]:
    if not isinstance(data, dict):
        return {"available": False}
    har = data.get("har_logs", data)
    entries = ((har.get("log") or {}).get("entries") or []) if isinstance(har, dict) else []
    if not isinstance(entries, list):
        return {"available": False}
    statuses: Counter[str] = Counter()
    paths: Counter[str] = Counter()
    graphql_shapes: list[dict[str, Any]] = []
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        request = entry.get("request") or {}
        response = entry.get("response") or {}
        location = safe_location(request.get("url"))
        if location.get("host") not in {"instagram.com", "www.instagram.com", "i.instagram.com"}:
            continue
        statuses[str(response.get("status") or 0)] += 1
        paths[location.get("path") or "/"] += 1
        if location.get("path") in {"/api/graphql", "/graphql/query"}:
            bodies = list(_bodies(response.get("content") or {}))
            comment_candidates = []
            media_codes = set()
            for body in bodies:
                for obj in _objects(body):
                    if (obj.get("pk") is not None or obj.get("id") is not None) and "text" in obj:
                        comment_candidates.append(str(obj.get("pk") or obj.get("id")))
                    code = obj.get("code") or obj.get("shortcode")
                    if isinstance(code, str):
                        media_codes.add(code)
            key_paths = set()
            pending = [(body, "", 0) for body in bodies]
            while pending:
                value, prefix, depth = pending.pop()
                if depth > 5:
                    continue
                if isinstance(value, dict):
                    for key, child_value in value.items():
                        path = f"{prefix}.{key}" if prefix else str(key)
                        key_paths.add(path)
                        if isinstance(child_value, (dict, list)):
                            pending.append((child_value, path, depth + 1))
                elif isinstance(value, list):
                    for child_value in value[:3]:
                        if isinstance(child_value, (dict, list)):
                            pending.append((child_value, f"{prefix}[]", depth + 1))
            graphql_shapes.append({
                "status": response.get("status"),
                "jsonDocuments": len(bodies),
                "topLevelKeys": [sorted(str(key) for key in body)[:30] for body in bodies],
                "keyPaths": sorted(key_paths)[:150],
                "commentCandidates": len(set(comment_candidates)),
                "mediaCodes": sorted(media_codes)[:20],
            })
    result = {
        "available": True,
        "entries": len(entries),
        "instagramStatuses": dict(statuses),
        "instagramPaths": dict(paths.most_common(30)),
        "graphql": graphql_shapes,
    }
    if media_id:
        capture = NetworkCapture(media_id=media_id)
        capture.ingest(data)
        result["parsed"] = {
            "comments": len(capture.comments),
            "nativeConnections": sorted(capture.native),
            "hasFirstRestPage": capture.first_comment_page is not None,
            "replyFirstPages": len(capture.reply_pages),
            "mediaCodes": sorted(capture._media_cache),
            "responsesExamined": capture.responses_seen,
        }
    return result


def summarize_step(step: dict[str, Any], media_id: str | None = None,
                   request: dict[str, Any] | None = None) -> dict[str, Any]:
    name = str(step.get("command") or "unknown")
    result: dict[str, Any] = {
        "command": name,
        "success": bool(step.get("success")),
        "offloaded": bool(step.get("offloaded_data_url")),
    }
    if step.get("error_name"):
        result["errorName"] = str(step["error_name"])
    if step.get("error_message"):
        result["errorMessage"] = str(step["error_message"])[:300]
    params = (request or {}).get("params") or {}
    if name in {"fetch_json", "open_url"} and isinstance(params, dict):
        result["requestLocation"] = safe_location(params.get("url"))
        if name == "fetch_json":
            result["requestMethod"] = str(params.get("method") or "GET").upper()
    # These are the only response bodies that are useful and safe to describe.
    # Cookie injection/dumps and run_js can contain credentials or live tokens.
    if name in {"fetch_json", "dump_har_logs"}:
        data = step_data(step)
        result["response"] = (summarize_fetch(data) if name == "fetch_json"
                              else summarize_har(data, media_id))
    elif name == "run_js":
        data = step_data(step)
        value = data.get("result") if isinstance(data, dict) else None
        decoded = None
        if isinstance(value, str) and value.lstrip().startswith("{"):
            try:
                decoded = json.loads(value)
            except ValueError:
                pass
        if isinstance(decoded, dict) and any(key in decoded for key in ("lsd", "csrf", "userId")):
            result["response"] = {
                "tokenProbe": {f"{key}Present": bool(decoded.get(key))
                               for key in ("lsd", "appId", "csrf", "userId")},
                "pageSignals": {key: decoded.get(key) for key in (
                    "loggedInMarker", "loginForm", "loginPrompt", "loggedOutClass",
                    "restricted", "pageLoaded", "proxyError") if key in decoded},
            }
        elif isinstance(value, (bool, int, float)) or value is None:
            result["response"] = {"scalar": value}
    elif name in {"open_url", "get_url"}:
        data = step_data(step)
        if isinstance(data, dict):
            result["location"] = safe_location(data.get("url"))
    return result


def inspect(session_id: str, media_id: str | None = None) -> dict[str, Any]:
    load_dotenv()
    config = build_input({"directUrls": ["https://www.instagram.com/"]})
    def admin_get(path: str) -> dict[str, Any]:
        # Post-mortem tool only: the Admin API is never used by the scraper.
        response = requests.get(f"{config.bro.base_url}{path}", timeout=60, headers={
            "Authorization": f"Bearer {config.bro.api_key}", "Accept": "application/json",
            "User-Agent": USER_AGENT})
        response.raise_for_status()
        return response.json()

    session_detail = admin_get(f"/v1/admin/sessions/{session_id}")
    session_record = session_detail.get("session") or session_detail
    session_key_paths = set()
    pending = [(session_record, "", 0)]
    while pending:
        value, prefix, depth = pending.pop()
        if not isinstance(value, dict) or depth > 3:
            continue
        for key, child in value.items():
            path = f"{prefix}.{key}" if prefix else str(key)
            session_key_paths.add(path)
            if isinstance(child, dict):
                pending.append((child, path, depth + 1))
    listed = admin_get(f"/v1/admin/sessions/{session_id}/commands?limit=100")
    commands = listed.get("commands") or []
    result: dict[str, Any] = {
        "sessionId": session_id,
        "session": {
            "status": session_record.get("status"),
            "params": {key: (session_record.get("params") or {}).get(key)
                       for key in ("enable_proxy", "country", "city", "proxy_tier",
                                   "proxy_policy", "block_unproxied")},
        },
        "commandCount": len(commands),
        "hasMore": bool(listed.get("has_more")),
        "adminSessionKeyPaths": sorted(session_key_paths),
        "commands": [],
    }
    for listed_command in commands:
        command_id = str(listed_command.get("command_id") or listed_command.get("_id") or "")
        if not command_id:
            continue
        detail = admin_get(f"/v1/admin/commands/{command_id}")
        command = detail.get("command") or {}
        response = command.get("response") or {}
        steps = response.get("commands") or []
        payload = command.get("payload") or []
        result["commands"].append({
            "commandId": command_id,
            "status": command.get("status"),
            "createdAt": command.get("created_at"),
            "finishedAt": command.get("finished_at"),
            "steps": [summarize_step(step, media_id,
                                     payload[index] if index < len(payload) and isinstance(payload[index], dict) else None)
                      for index, step in enumerate(steps) if isinstance(step, dict)],
        })
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("session_id")
    parser.add_argument("--media-id")
    args = parser.parse_args()
    print(json.dumps(inspect(args.session_id, args.media_id), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
