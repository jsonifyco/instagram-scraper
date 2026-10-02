"""Separate managed requests from other observed browser traffic in HAR."""
from collections import Counter
from datetime import datetime
import hashlib
import json
import time
from urllib.parse import urlsplit, parse_qsl


def fingerprint(method, url, body):
    parts = urlsplit(url)
    normalized = (method.upper(), parts.hostname, parts.path.rstrip("/"), sorted(parse_qsl(parts.query)), sorted(parse_qsl(body or "")))
    return hashlib.sha256(json.dumps(normalized).encode()).hexdigest()


class Traffic:
    def __init__(self):
        self.started = time.time()
        self.pending = Counter()
        self.seen = set()
        self.observed = self.matched = 0
        self.truncated = False
        self.last_observed = None
        #: set by the collector when the in-page observer is the source: no
        #: periodic HAR dumps, so this accounting covers only the dumps that
        #: still happen (initial load, verification) and is partial
        self.partial = False
        #: set by the runner when every HAR export of the run is narrowed to
        #: these resource types (``har_filtered``): the counts below cover
        #: only requests of those types
        self.resource_types = None
        self.managed_keys = set()

    def managed(self, url, method="GET", body=None):
        key = fingerprint(method, url, body)
        self.pending[key] += 1
        # Every request this process ever issued through the browser: the
        # observer verification must not mistake our own managed reads
        # (getbro's fetch_json, outside the page's fetch) for UI traffic.
        if len(self.managed_keys) < 50000:
            self.managed_keys.add(key)

    def is_managed(self, url, method="GET", body=None):
        return fingerprint(method, url, body) in self.managed_keys

    def observe(self, payload):
        if not isinstance(payload, dict):
            return
        har = payload.get("har_logs", payload)
        if not isinstance(har, dict) or not isinstance(har.get("log"), dict):
            return
        entries = har["log"].get("entries") or []
        if not isinstance(entries, list):
            return
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            if len(self.seen) >= 20000:
                self.truncated = True
                break
            try:
                started = datetime.fromisoformat(entry.get("startedDateTime", "").replace("Z", "+00:00")).timestamp()
            except (ValueError, AttributeError, TypeError):
                continue
            if started < self.started:
                continue
            req = entry.get("request") or {}
            if not isinstance(req, dict) or not isinstance(req.get("postData", {}), dict):
                continue
            key = fingerprint(req.get("method", "GET"), req.get("url", ""), (req.get("postData") or {}).get("text"))
            signature = (entry["startedDateTime"], key, entry.get("time"))
            if signature in self.seen:
                continue
            self.seen.add(signature)
            self.observed += 1
            if self.pending[key]:
                self.pending[key] -= 1
                self.matched += 1
        self.last_observed = time.time()

    def summary(self):
        scope = ("Partial: this collection mode omits or limits HAR collection. "
                 "Only exported HAR requests are counted; take cost from billing "
                 "after the VM stopped"
                 if self.partial else
                 f"Limited to HAR entries of resource types {list(self.resource_types)} "
                 "exported during this invocation; requests of other types are not "
                 "counted and unmatched traffic may include managed requests"
                 if self.resource_types else
                 "Only requests present in HAR during this invocation; unmatched traffic may include managed requests")
        return {"harObservedRequests": self.observed, "managedRequestsMatched": self.matched,
                "otherBrowserRequestsObserved": self.observed - self.matched,
                "managedRequestsNotYetMatched": sum(self.pending.values()),
                "lastObservedAt": self.last_observed, "truncated": self.truncated,
                "partial": self.partial,
                "resourceTypes": list(self.resource_types) if self.resource_types else None,
                "scope": scope}
