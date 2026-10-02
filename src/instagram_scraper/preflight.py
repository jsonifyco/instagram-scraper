"""Anonymous route verification before account cookies enter the browser."""
from __future__ import annotations
import ipaddress
import json
import re
from urllib.parse import urlsplit
from .bro import commands as cmd
from .errors import FatalError, BroError
from .budget import BudgetExceeded


def network_facts(value):
    """Keep diagnostic codes, not arbitrary proxy messages or credentials."""
    text = str(value or "")
    return {"networkCodes": sorted(set(re.findall(r"\bERR_[A-Z_]+\b", text))),
            "httpCodes": sorted(set(re.findall(r"(?:HTTP(?:/\d(?:\.\d)?)?|status|CONNECT)\s*[:=]?\s*([45]\d\d)\b", text, re.I))),
            "connectFailure": bool(re.search(r"(?:CONNECT|ERR_TUNNEL|ERR_PROXY|upstream proxy)", text, re.I)),
            "timeout": "timeout" in text.lower() or "timed out" in text.lower()}


def _error_facts(exc):
    facts = network_facts(exc)
    for source, target in (("name", "errorName"), ("command", "command")):
        value = getattr(exc, source, None)
        if isinstance(value, str) and re.fullmatch(r"[A-Za-z0-9_. -]{1,100}", value):
            facts[target] = value
    return facts


def _page_facts(ctx):
    try:
        return network_facts(ctx.session.js("document.body.innerText", out_type="str"))
    except BroError as exc:
        return {"inspectionError": type(exc).__name__, **_error_facts(exc)}


def _har_facts(ctx):
    try:
        payload = ctx.session.run_one(cmd.dump_har_logs(), retries=0)
        har = payload.get("har_logs", payload) if isinstance(payload, dict) else {}
        entries = (har.get("log") or {}).get("entries") or []
        result = []
        for entry in entries[-100:]:
            req, res = entry.get("request") or {}, entry.get("response") or {}
            url = urlsplit(req.get("url") or "")
            if url.hostname not in ("api.ipify.org", "www.instagram.com", "instagram.com"):
                continue
            result.append({"host": url.hostname, "path": url.path, "status": res.get("status"),
                           "statusFacts": network_facts(res.get("statusText")),
                           "bodyFacts": network_facts((res.get("content") or {}).get("text")),
                           "milliseconds": entry.get("time")})
        return result
    except BroError as exc:
        return [{"inspectionError": type(exc).__name__, **_error_facts(exc)}]
    except (AttributeError, TypeError, ValueError):
        return [{"inspectionError": "MalformedHAR"}]


def verify_route(ctx, report):
    report.update(ipCheck="pending", instagramCheck="pending", cookiesInjected=False)
    if ctx.budget:
        ctx.budget.check()
    try:
        ctx.session.goto("https://api.ipify.org/?format=json", wait=1)
        text = ctx.session.js("document.body.innerText", out_type="str")
        ipaddress.ip_address(json.loads(text)["ip"])
        report["ipCheck"] = "passed"
    except (BroError, ValueError, TypeError, KeyError) as exc:
        report["ipCheck"] = "failed"
        report["ipError"] = type(exc).__name__
        report["ipFailureDetails"] = _error_facts(exc)
        # Do not log arbitrary page bodies, tokens or proxy credentials.
        if "CONNECT" in str(exc) or "proxy" in str(exc).lower():
            report["upstreamConnect"] = "failed"
        try:
            text = str(ctx.session.js("document.body.innerText", out_type="str") or "")
            if "CONNECT" in text or "Upstream proxy" in text or "ERR_PROXY" in text:
                report["upstreamConnect"] = "failed"
                for code in ("407", "412", "502", "503"):
                    if code in text:
                        report["upstreamStatus"] = code
                        break
        except BroError:
            pass
    try:
        ctx.bootstrap()
        loaded = ctx.session.js(
            "!!document.querySelector('script[src*=\"cdninstagram.com\"], script[data-sjs]')",
            out_type="bool")
        if loaded is not True:
            raise FatalError("Instagram did not render")
        report["instagramCheck"] = "passed"
    except BudgetExceeded:
        raise
    except (BroError, FatalError) as exc:
        report["instagramCheck"] = "failed"
        report["instagramError"] = type(exc).__name__
        report["instagramFailureDetails"] = _error_facts(exc)
        report["instagramPageDetails"] = _page_facts(ctx)
        if "proxy" in str(exc).lower() or "CONNECT" in str(exc):
            report["upstreamConnect"] = "failed"
    if report["ipCheck"] != "passed" or report["instagramCheck"] != "passed":
        report["anonymousTraffic"] = _har_facts(ctx)
        raise FatalError("Anonymous route check failed (exit IP or Instagram did not load); "
                         "cookies not injected")
