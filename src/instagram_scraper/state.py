"""Transactional, disk-backed collection, work queue and atomic exports."""
from __future__ import annotations
from contextlib import contextmanager, closing
import csv
import hashlib
import json
import logging
import os
from pathlib import Path
import sqlite3
import time
import threading
import uuid

from .errors import InputError
from .storage import KeyValueStore, _csv_cell

log = logging.getLogger(__name__)


def encode(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), default=str)


def merge_fields(old, new, meta, source, observed, prefix="", known=None):
    """Sparse values cannot erase observations; counters follow observation time."""
    result = dict(old)
    for name, value in new.items():
        path = f"{prefix}.{name}" if prefix else name
        previous = meta.get(path, {})
        if isinstance(value, dict):
            result[name] = merge_fields(old.get(name) if isinstance(old.get(name), dict) else {},
                                        value, meta, source, observed, path, known)
            continue
        explicit = known is not None and path in known
        if value is None or value in ([], ""):
            if not explicit:
                result.setdefault(name, value)
                meta.setdefault(path, {"status": "not_requested"})
                continue
        # Nested entities also arrive as sparse previews. Merge their fields by
        # identity, so a thin carousel/comment response cannot erase attachments.
        if name in ("replies", "childPosts", "latestComments", "taggedUsers", "coauthorProducers") and isinstance(value, list) and value:
            by_id = {str(r.get("id") or r.get("username")): r for r in (old.get(name) or [])
                     if isinstance(r, dict) and (r.get("id") or r.get("username"))}
            unidentified = []
            for row in value:
                identity = str(row.get("id") or row.get("username") or "") if isinstance(row, dict) else ""
                if identity:
                    by_id[identity] = merge_fields(by_id.get(identity, {}), row, meta, source, observed,
                                                   prefix=f"{path}.{identity}", known=known)
                else:
                    unidentified.append(row)
            value = list(by_id.values()) + unidentified
        elif previous.get("status") in ("value", "empty") and previous.get("observedAt", 0) > observed:
            continue
        result[name] = value
        meta[path] = {"status": "empty" if value is None or value in ([], "") else "value",
                      "source": source if observed >= previous.get("observedAt", 0) else previous.get("source", source),
                      "observedAt": max(observed, previous.get("observedAt", 0)) if previous.get("status") in ("value", "empty") else observed}
    return result


class StateStore:
    def __init__(self, path: Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(self.path, isolation_level=None)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS records(
                key TEXT PRIMARY KEY, kind TEXT NOT NULL, scope TEXT NOT NULL,
                payload TEXT NOT NULL, fields TEXT NOT NULL);
            CREATE INDEX IF NOT EXISTS records_scope ON records(scope,kind);
            -- These queries otherwise decode every comment/owner JSON row on
            -- each reply page or owner enrichment. Indexes are additive, so
            -- old checkpoints gain them when they are opened for resume.
            CREATE INDEX IF NOT EXISTS records_reply_parent ON records(
                scope,json_extract(payload,'$.parentCommentId')) WHERE kind='comment';
            CREATE INDEX IF NOT EXISTS records_owner_id ON records(
                json_extract(payload,'$.owner.id'));
            CREATE INDEX IF NOT EXISTS records_owner_username ON records(
                json_extract(payload,'$.ownerUsername'));
            CREATE INDEX IF NOT EXISTS records_owner_flat_id ON records(
                json_extract(payload,'$.ownerId'));
            CREATE TABLE IF NOT EXISTS jobs(
                id TEXT PRIMARY KEY, kind TEXT NOT NULL, scope TEXT NOT NULL,
                payload TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending', turn INTEGER NOT NULL);
            CREATE INDEX IF NOT EXISTS jobs_queue ON jobs(status,kind,turn);
            CREATE INDEX IF NOT EXISTS jobs_turn ON jobs(turn);
            CREATE TABLE IF NOT EXISTS targets(id TEXT PRIMARY KEY, status TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS cache(key TEXT PRIMARY KEY, payload TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS command_operations(
                id TEXT PRIMARY KEY, job_id TEXT, session_id TEXT, command_id TEXT,
                operation_kind TEXT NOT NULL, request_fingerprint TEXT NOT NULL,
                attempt INTEGER NOT NULL, state TEXT NOT NULL, phase TEXT,
                last_status TEXT, error_type TEXT, created_at REAL NOT NULL,
                updated_at REAL NOT NULL, response_at REAL, applied_at REAL);
            CREATE INDEX IF NOT EXISTS command_operations_job
                ON command_operations(job_id,state,updated_at);
            CREATE UNIQUE INDEX IF NOT EXISTS command_operations_command
                ON command_operations(session_id,command_id)
                WHERE command_id IS NOT NULL;
            CREATE TABLE IF NOT EXISTS command_operation_events(
                id INTEGER PRIMARY KEY AUTOINCREMENT, operation_id TEXT NOT NULL,
                state TEXT NOT NULL, phase TEXT, last_status TEXT,
                error_type TEXT, observed_at REAL NOT NULL);
            CREATE INDEX IF NOT EXISTS command_operation_events_operation
                ON command_operation_events(operation_id,id);
            CREATE TABLE IF NOT EXISTS pending_pages(
                job_id TEXT PRIMARY KEY, operation_id TEXT, source TEXT NOT NULL,
                payload TEXT NOT NULL, created_at REAL NOT NULL, applied_at REAL);
            CREATE TABLE IF NOT EXISTS har_responses(
                signature TEXT PRIMARY KEY, observed_at REAL NOT NULL);
            CREATE TABLE IF NOT EXISTS ui_thread_state(
                scope TEXT NOT NULL, parent_id TEXT NOT NULL,
                payload TEXT NOT NULL, updated_at REAL NOT NULL,
                PRIMARY KEY(scope,parent_id));
            CREATE TABLE IF NOT EXISTS observed_pages(
                id TEXT PRIMARY KEY, job_id TEXT NOT NULL, operation_id TEXT,
                source TEXT NOT NULL, payload TEXT NOT NULL,
                created_at REAL NOT NULL, applied_at REAL);
            CREATE INDEX IF NOT EXISTS observed_pages_pending
                ON observed_pages(applied_at,created_at);
            CREATE TABLE IF NOT EXISTS ui_parent_order(
                scope TEXT NOT NULL, parent_id TEXT NOT NULL,
                position INTEGER NOT NULL, loaded_at REAL NOT NULL, scanned_at REAL,
                origin TEXT NOT NULL DEFAULT 'network', dom_status TEXT,
                present_at REAL, control_at REAL, dom_checked_at REAL,
                focus_attempts INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY(scope,parent_id));
            CREATE INDEX IF NOT EXISTS ui_parent_order_position
                ON ui_parent_order(scope,position);
            CREATE TABLE IF NOT EXISTS ui_parent_observations(
                scope TEXT NOT NULL,parent_id TEXT NOT NULL,invocation INTEGER NOT NULL,
                document INTEGER NOT NULL,first_present_at REAL,viewport_at REAL,
                control_at REAL,immediate_at REAL,terminal_at REAL,
                last_checked_at REAL NOT NULL,last_status TEXT NOT NULL,
                checks INTEGER NOT NULL DEFAULT 1,
                PRIMARY KEY(scope,parent_id,invocation,document));
            CREATE INDEX IF NOT EXISTS ui_parent_observations_status
                ON ui_parent_observations(scope,invocation,document,last_status);
        """)
        columns = {row[1] for row in self.db.execute("PRAGMA table_info(ui_parent_order)")}
        for name, declaration in (
                ("origin", "TEXT NOT NULL DEFAULT 'unknown'"),
                ("dom_status", "TEXT"), ("present_at", "REAL"),
                ("control_at", "REAL"), ("dom_checked_at", "REAL"),
                ("focus_attempts", "INTEGER NOT NULL DEFAULT 0")):
            if name not in columns:
                self.db.execute(f"ALTER TABLE ui_parent_order ADD COLUMN {name} {declaration}")
        observation_columns = {
            row[1] for row in self.db.execute("PRAGMA table_info(ui_parent_observations)")}
        for name in ("immediate_at", "terminal_at"):
            if name not in observation_columns:
                self.db.execute(
                    f"ALTER TABLE ui_parent_observations ADD COLUMN {name} REAL")
        # Old checkpoints did not distinguish network rows from parent IDs
        # inferred from the DOM. A saved parent record is authoritative.
        self.db.execute("""UPDATE ui_parent_order SET origin=CASE WHEN EXISTS(
            SELECT 1 FROM records r WHERE r.kind='comment' AND r.scope=ui_parent_order.scope
            AND json_extract(r.payload,'$.id')=ui_parent_order.parent_id
            AND json_extract(r.payload,'$.parentCommentId') IS NULL)
            THEN 'network' ELSE 'structural' END WHERE origin='unknown'""")

    @contextmanager
    def transaction(self):
        self.db.execute("BEGIN IMMEDIATE")
        try:
            yield
            self.db.execute("COMMIT")
        except BaseException:
            self.db.execute("ROLLBACK")
            raise

    @contextmanager
    def _write_batch(self):
        """Keep a local row batch atomic without nesting a page transaction.

        The connection uses FULL durability and autocommit. Per-row DOM
        bookkeeping otherwise performs hundreds of synchronous commits for
        each screen; it must be durable as one observation instead.
        """
        if self.db.in_transaction:
            yield
        else:
            with self.transaction():
                yield

    def set_meta(self, key, value):
        self.db.execute("INSERT OR REPLACE INTO meta VALUES(?,?)", (key, encode(value)))

    def get_meta(self, key, default=None):
        row = self.db.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return json.loads(row[0]) if row else default

    def bind(self, config, *, resume=False):
        fingerprint = {k: config.to_dict()[k] for k in (
            "direct_urls", "search", "search_type", "search_limit", "results_type",
            "only_posts_newer_than", "until_date", "is_newest_comments", "include_nested_comments")}
        digest = hashlib.sha256(encode(fingerprint).encode()).hexdigest()
        previous = self.get_meta("fingerprint")
        if previous and not resume:
            raise InputError("Complete-mode output already contains state; use --resume or a new output directory")
        if previous and previous != digest:
            raise InputError("Resume targets, sorting, result type or date filters differ from the saved run")
        if resume and not previous:
            raise InputError("No initialized checkpoint exists in the resume directory")
        previous_input = self.get_meta("input", {})
        invocation = int(self.get_meta("invocation", 0)) + 1
        self.set_meta("invocation", invocation)
        parents = self.db.execute("SELECT count(*) FROM records WHERE kind='comment' AND json_extract(payload,'$.parentCommentId') IS NULL").fetchone()[0]
        replies = self.db.execute("SELECT count(*) FROM records WHERE kind='comment' AND json_extract(payload,'$.parentCommentId') IS NOT NULL").fetchone()[0]
        self.set_meta("invocationBaseline", {"parents": parents, "replies": replies,
                                              "startedAt": time.time()})
        self.set_meta("invocationMetrics", {})
        if resume:
            old_limit = previous_input.get("comment_post_limit") or previous_input.get("results_limit", 0)
            new_limit = config.comment_post_limit or config.results_limit
            if new_limit > old_limit:
                self.db.execute("UPDATE targets SET status='pending'")
        self.set_meta("fingerprint", digest)
        self.set_meta("input", config.to_dict())
        if resume:
            # A task the previous invocation deferred because a getbro command
            # failed (a read timeout, a lost payload) is what an explicit
            # resume is for: it is re-opened once, with its cursor intact.
            # Instagram-side stops (limits, gaps, unavailability) stay as they are.
            self.reopen_command_failures()
            # A confirmed browser cancellation keeps its cursor. Only an
            # explicit resume may try it again; no same-run retry loop.
            for row in self.db.execute("SELECT * FROM jobs WHERE status='blocked' "
                    "AND json_extract(payload,'$.stopReason')='PageFetchReadTimeoutError'").fetchall():
                job = dict(row)
                job['payload'] = json.loads(job['payload'])
                job['payload'].pop('stopReason', None)
                self.save_job(job, status='pending')
            # Direct work established by an earlier DOM audit can run before
            # a new ranked pass. Recover explicit REST terminal evidence from
            # old checkpoints too; a reported-count gap is not a new cursor.
            for row in self.db.execute("SELECT id,payload FROM jobs WHERE kind='replies' AND status!='done'").fetchall():
                payload = json.loads(row['payload'])
                saved = self.db.execute(
                    "SELECT payload FROM pending_pages WHERE job_id=? AND applied_at IS NOT NULL",
                    (row['id'],)).fetchone()
                body = json.loads(saved[0]) if saved else {}
                if (payload.get('source') == 'api' and
                        body.get('has_more_head_child_comments') is False and
                        body.get('has_more_tail_child_comments') is False and
                        not body.get('next_min_child_cursor') and not body.get('next_max_child_cursor') and
                        not payload.get('queue')):
                    payload['serverExhaustionConfirmed'] = True
                if payload.get('stopReason') == 'parent_not_rendered_in_dom':
                    payload.update(source='direct_ui', queue=[{}], seenCursors=[])
                if (not payload.get('serverExhaustionConfirmed') and
                        (payload.get('source') == 'direct_ui' or
                         payload.get('directRestContinuation') and payload.get('queue'))):
                    payload['resumeDirectFirst'] = invocation
                self.db.execute("UPDATE jobs SET payload=? WHERE id=?", (encode(payload), row['id']))
            if (config.collection_phase != "enrich" and
                    config.revisit_ranked_parents_on_resume):
                for row in self.db.execute("SELECT id,payload FROM jobs WHERE kind='parents'").fetchall():
                    payload = json.loads(row["payload"])
                    payload.update(queue=[{}], seenCursors=[], pages=0, dataPages=0,
                                   emptyPages=0, preloaded=False, source="rest",
                                   parentPass=int(payload.get("parentPass", 1)) + 1,
                                   uiExhaustionConfirmed=False, uiNoLinksStreak=0,
                                   uiPhase="opening", discoveredThreads=[])
                    for key in ("stopReason", "uiEndCursor", "uiSeekCursor", "template",
                                "uiProgress"):
                        payload.pop(key, None)
                    self.db.execute("UPDATE jobs SET status='pending',payload=? WHERE id=?",
                                    (encode(payload), row["id"]))
            needs_thread_pass = self.db.execute("""SELECT 1 FROM jobs
                WHERE kind='replies' AND status!='done' AND
                json_extract(payload,'$.source')='native_ui' AND
                coalesce(json_extract(payload,'$.serverExhaustionConfirmed'),0)=0 AND
                coalesce(json_extract(payload,'$.stopReason'),'') IN
                ('reported_count_gap','ui_stalled','ui_scroll_stalled',
                 'ui_pagination_unresponsive','ui_region_unavailable','ui_control_absent',
                 'awaiting_parent_pass','reply_click_cap','ambiguous_thread_boundary',
                 'reply_response_missing','misattributed') LIMIT 1""").fetchone()
            if needs_thread_pass and config.collection_phase != "enrich":
                for row in self.db.execute(
                        "SELECT id,payload FROM jobs WHERE kind='parents' AND status='done'").fetchall():
                    payload = json.loads(row["payload"])
                    payload.update(queue=[{}], seenCursors=[], pages=0, dataPages=0,
                                   emptyPages=0, preloaded=False, source="rest",
                                   recoveryThreadPass=invocation,
                                   uiExhaustionConfirmed=False, uiPhase="opening")
                    for key in ("stopReason", "uiEndCursor", "uiSeekCursor", "template",
                                "uiProgress"):
                        payload.pop(key, None)
                    self.db.execute("UPDATE jobs SET status='pending',payload=? WHERE id=?",
                                    (encode(payload), row["id"]))
            # Older UI collectors treated a missing scroll container as done.
            # Reopen only unproven UI completion on an explicit invocation.
            for row in self.db.execute("SELECT id,payload FROM jobs WHERE kind='parents' AND status='done'").fetchall():
                payload = json.loads(row['payload'])
                if (config.collection_phase != "enrich" and
                        payload.get('source') == 'native_ui' and not payload.get('uiExhaustionConfirmed')):
                    payload.update(queue=[{}], emptyPages=0)
                    self.db.execute("UPDATE jobs SET status='pending',payload=? WHERE id=?", (encode(payload), row['id']))
            # One explicit invocation retries blocked jobs once; successful
            # enrichment and exhausted lists are never reissued automatically.
            for row in self.db.execute("SELECT id,kind,payload,status FROM jobs WHERE status!='done'"):
                payload = json.loads(row["payload"])
                if config.collection_phase == "enrich" and not payload.get("type"):
                    continue
                if payload.get('serverExhaustionConfirmed'):
                    # Keep the count discrepancy visible without issuing the
                    # exhausted list again on every explicit resume.
                    continue
                payload["totalPages"] = payload.get("totalPages", 0) + payload.get("pages", 0)
                payload["pages"] = 0
                payload["reopensThisInvocation"] = 0
                payload.pop("cursorReset", None)
                if (needs_thread_pass and row["kind"] == "replies" and
                        payload.get("source") == "native_ui"):
                    # Reply controls are discovered and drained by the common
                    # parent-list pass. Scheduling these jobs separately would
                    # revive the removed per-parent DOM search.
                    payload.update(queue=[], stopReason="awaiting_parent_pass")
                    self.db.execute(
                        "UPDATE jobs SET status='blocked',payload=? WHERE id=?",
                        (encode(payload), row["id"]))
                    continue
                if payload.get("source") == "native_ui":
                    # ui_round is a local scheduling token, not a server cursor.
                    # A new invocation restarts its counter; old tokens would
                    # otherwise cause a false cursor_cycle on the first page.
                    payload.update(queue=[{}], seenCursors=[], emptyPages=0)
                    payload.pop("uiProgress", None)
                    # Older checkpoints counted DOM movements as pages. The
                    # new limit counts only decoded parent/reply responses.
                    payload.setdefault("dataPages", 0)
                    payload.pop("uiEndCursor", None)
                    payload.pop("uiSeekCursor", None)
                elif payload.get("source") == "direct_ui":
                    payload.update(queue=[{}], seenCursors=[], emptyPages=0)
                    payload.pop("directUiAttemptedInvocation", None)
                if payload.get("stopReason") in (
                        "cursor_cycle", "no_progress", "ui_stalled",
                        "ui_scroll_stalled", "ui_pagination_unresponsive",
                        "ui_region_unavailable", "missing_cursor",
                        "reported_count_gap"):
                    payload.update(queue=[{}], seenCursors=[], emptyPages=0)
                    payload.pop("uiProgress", None)
                payload.pop("stopReason", None)
                self.db.execute("UPDATE jobs SET status='pending',payload=? WHERE id=?", (encode(payload), row["id"]))

    def count(self, scope=None):
        if scope is None:
            return self.db.execute("SELECT count(*) FROM records").fetchone()[0]
        return self.db.execute("SELECT count(*) FROM records WHERE scope=?", (str(scope),)).fetchone()[0]

    def upsert(self, record, *, scope, kind, source="api", observed=None, known=None):
        if kind == "post" and record.get("id"):
            # Media info may use a composite mediaId_ownerId for the same tile.
            record = {**record, "id": str(record["id"]).split("_", 1)[0]}
        identity = record.get("id") or record.get("shortCode") or record.get("url")
        if identity is None:
            identity = hashlib.sha256(encode(record).encode()).hexdigest()
        key = f"{kind}:{scope}:{identity}"
        row = self.db.execute("SELECT payload,fields FROM records WHERE key=?", (key,)).fetchone()
        old, fields = (json.loads(row[0]), json.loads(row[1])) if row else ({}, {})
        merged = merge_fields(old, record, fields, source, time.time() if observed is None else observed, known=known)
        self.db.execute("""INSERT INTO records VALUES(?,?,?,?,?) ON CONFLICT(key)
                           DO UPDATE SET payload=excluded.payload,fields=excluded.fields""",
                        (key, kind, str(scope), encode(merged), encode(fields)))
        if row is None and self.get_meta("firstResultThisInvocation") is None:
            self.set_meta("firstResultThisInvocation", time.time())
        return key

    def exists(self, kind, scope, identity):
        return self.db.execute("SELECT 1 FROM records WHERE key=?", (f"{kind}:{scope}:{identity}",)).fetchone() is not None

    def get_record(self, key):
        row = self.db.execute("SELECT * FROM records WHERE key=?", (key,)).fetchone()
        return {**dict(row), "payload": json.loads(row["payload"]), "fields": json.loads(row["fields"])} if row else None

    def rows(self, scope=None):
        query = "SELECT payload FROM records" + (" WHERE scope=?" if scope is not None else "") + " ORDER BY rowid"
        for row in self.db.execute(query, (str(scope),) if scope is not None else ()):
            yield json.loads(row[0])

    def mark_unavailable(self, key, fields, source):
        row = self.get_record(key)
        if not row:
            return
        metadata = row["fields"]
        for field in fields:
            if metadata.get(field, {}).get("status", "not_requested") == "not_requested":
                metadata[field] = {"status": "unavailable", "source": source, "observedAt": time.time()}
        self.db.execute("UPDATE records SET fields=? WHERE key=?", (encode(metadata), key))

    def enqueue(self, identity, kind, scope, payload):
        turn = self.db.execute("SELECT coalesce(max(turn),0)+1 FROM jobs").fetchone()[0]
        self.db.execute("INSERT OR IGNORE INTO jobs VALUES(?,?,?,?,?,?)",
                        (identity, kind, str(scope), encode(payload), "pending", turn))

    def next_job(self, kind, *, include_scopes=(), exclude_scopes=(), exclude_ids=(),
                 exclude_sources=()):
        query = "SELECT * FROM jobs WHERE status='pending' AND kind=?"
        args = [kind]
        if exclude_sources:
            query += (" AND coalesce(json_extract(payload,'$.source'),'') NOT IN ("
                      + ",".join("?" * len(exclude_sources)) + ")")
            args.extend(exclude_sources)
        if include_scopes:
            query += " AND scope IN (" + ",".join("?" * len(include_scopes)) + ")"
            args.extend(include_scopes)
        if exclude_scopes:
            query += " AND scope NOT IN (" + ",".join("?" * len(exclude_scopes)) + ")"
            args.extend(exclude_scopes)
        if exclude_ids:
            query += " AND id NOT IN (" + ",".join("?" * len(exclude_ids)) + ")"
            args.extend(exclude_ids)
        row = self.db.execute(query + " ORDER BY turn LIMIT 1", args).fetchone()
        return {**dict(row), "payload": json.loads(row["payload"])} if row else None

    def save_job(self, job, *, status="pending"):
        turn = self.db.execute("SELECT coalesce(max(turn),0)+1 FROM jobs").fetchone()[0]
        self.db.execute("UPDATE jobs SET payload=?,status=?,turn=? WHERE id=?",
                        (encode(job["payload"]), status, turn, job["id"]))

    def cache_get(self, key):
        row = self.db.execute("SELECT payload FROM cache WHERE key=?", (key,)).fetchone()
        return json.loads(row[0]) if row else None

    def cache_set(self, key, payload):
        self.db.execute("INSERT OR REPLACE INTO cache VALUES(?,?)", (key, encode(payload)))

    def claim_har_response(self, signature, observed_at=None):
        """Persistently claim a decoded HAR version without storing its secrets."""
        before = self.db.total_changes
        self.db.execute("INSERT OR IGNORE INTO har_responses VALUES(?,?)",
                        (str(signature), time.time() if observed_at is None else observed_at))
        return self.db.total_changes > before

    def has_har_response(self, signature):
        return self.db.execute("SELECT 1 FROM har_responses WHERE signature=?",
                               (str(signature),)).fetchone() is not None

    # -- loaded parent order: the traversal's map of the list -------------

    def append_parent_order(self, scope, parent_ids, *, origin="network"):
        """Record parents in the order the network delivered them."""
        row = self.db.execute("SELECT coalesce(max(position),-1) FROM ui_parent_order WHERE scope=?",
                              (str(scope),)).fetchone()
        position = int(row[0]) + 1
        now = time.time()
        for parent_id in parent_ids:
            existing = self.db.execute(
                "SELECT origin FROM ui_parent_order WHERE scope=? AND parent_id=?",
                (str(scope), str(parent_id))).fetchone()
            if existing:
                if origin == "network" and existing[0] != "network":
                    self.db.execute("UPDATE ui_parent_order SET origin='network' "
                                    "WHERE scope=? AND parent_id=?",
                                    (str(scope), str(parent_id)))
                continue
            self.db.execute("""INSERT INTO ui_parent_order(
                scope,parent_id,position,loaded_at,origin) VALUES(?,?,?,?,?)""",
                            (str(scope), str(parent_id), position, now, str(origin)))
            position += 1

    def parent_order(self, scope):
        order = [row[0] for row in self.db.execute(
            "SELECT parent_id FROM ui_parent_order WHERE scope=? ORDER BY position", (str(scope),))]
        if order:
            return order
        # A checkpoint written before the order table existed: the saved
        # parent records, in the order they were saved, are its best map.
        legacy = [str(row[0]) for row in self.db.execute(
            "SELECT json_extract(payload,'$.id') FROM records WHERE kind='comment' AND scope=? "
            "AND json_extract(payload,'$.parentCommentId') IS NULL ORDER BY rowid", (str(scope),))
            if row[0] is not None]
        if legacy:
            self.append_parent_order(scope, legacy)
        return legacy

    def mark_scanned(self, scope, parent_ids):
        now = time.time()
        with self._write_batch():
            for parent_id in parent_ids:
                self.db.execute("""UPDATE ui_parent_order SET scanned_at=coalesce(scanned_at,?),
                    present_at=coalesce(present_at,?),dom_checked_at=?,dom_status='visible'
                    WHERE scope=? AND parent_id=?""",
                                (now, now, now, str(scope), str(parent_id)))

    def mark_parent_dom_observations(self, scope, observations, *, invocation=None,
                                     document=0, stage="scan"):
        now = time.time()
        invocation = int(self.get_meta("invocation", 0) if invocation is None else invocation)
        with self._write_batch():
            self._write_parent_dom_observations(
                scope, observations, invocation=invocation, document=document,
                stage=stage, now=now)

    def _write_parent_dom_observations(self, scope, observations, *, invocation,
                                       document, stage, now):
        for row in observations:
            parent = str(row.get("parentId") or "")
            if not parent:
                continue
            present = bool(row.get("present"))
            control = bool(row.get("control"))
            status = "visible" if row.get("visible") else "present" if present else "not_rendered"
            self.db.execute("""UPDATE ui_parent_order SET dom_status=CASE
                WHEN ? THEN ? WHEN dom_status IN ('present','visible') THEN dom_status
                ELSE 'not_rendered' END,dom_checked_at=?,
                present_at=CASE WHEN ? THEN coalesce(present_at,?) ELSE present_at END,
                control_at=CASE WHEN ? THEN coalesce(control_at,?) ELSE control_at END
                WHERE scope=? AND parent_id=?""",
                (present, status, now, present, now, control, now, str(scope), parent))
            self.db.execute("""INSERT INTO ui_parent_observations(
                scope,parent_id,invocation,document,first_present_at,viewport_at,
                control_at,immediate_at,terminal_at,last_checked_at,last_status,checks)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,1)
                ON CONFLICT(scope,parent_id,invocation,document) DO UPDATE SET
                first_present_at=coalesce(ui_parent_observations.first_present_at,excluded.first_present_at),
                viewport_at=coalesce(ui_parent_observations.viewport_at,excluded.viewport_at),
                control_at=coalesce(ui_parent_observations.control_at,excluded.control_at),
                immediate_at=coalesce(ui_parent_observations.immediate_at,excluded.immediate_at),
                terminal_at=coalesce(ui_parent_observations.terminal_at,excluded.terminal_at),
                last_checked_at=excluded.last_checked_at,last_status=CASE
                WHEN excluded.last_status='visible' THEN 'visible'
                WHEN ui_parent_observations.last_status='visible' THEN 'visible'
                WHEN excluded.last_status='present' THEN 'present'
                WHEN ui_parent_observations.last_status='present' THEN 'present'
                ELSE excluded.last_status END,checks=ui_parent_observations.checks+1""",
                (str(scope), parent, invocation, int(document or 0),
                 now if present else None, now if row.get("visible") else None,
                 now if control else None, now if stage == "immediate" else None,
                 now if stage == "terminal" else None, now, status))

    def reset_parent_dom_observations(self, scope):
        self.db.execute("""UPDATE ui_parent_order SET dom_status=NULL,present_at=NULL,
            control_at=NULL,dom_checked_at=NULL,focus_attempts=0 WHERE scope=?""",
                        (str(scope),))

    def add_parent_focus_attempt(self, scope, parent_id):
        self.db.execute("UPDATE ui_parent_order SET focus_attempts=focus_attempts+1 "
                        "WHERE scope=? AND parent_id=?", (str(scope), str(parent_id)))
        row = self.db.execute("SELECT focus_attempts FROM ui_parent_order "
                              "WHERE scope=? AND parent_id=?",
                              (str(scope), str(parent_id))).fetchone()
        return int(row[0]) if row else 0

    def mark_parent_focus_failed(self, scope, parent_id):
        self.db.execute("UPDATE ui_parent_order SET dom_status='focus_failed',dom_checked_at=? "
                        "WHERE scope=? AND parent_id=?",
                        (time.time(), str(scope), str(parent_id)))

    def unresolved_parent_ids(self, scope, *, limit=None):
        query = ("SELECT parent_id FROM ui_parent_order WHERE scope=? AND scanned_at IS NULL "
                 "AND coalesce(dom_status,'')!='not_rendered' ORDER BY position")
        args = [str(scope)]
        if limit is not None:
            query += " LIMIT ?"
            args.append(int(limit))
        return [str(row[0]) for row in self.db.execute(query, args)]

    def parent_scan_diagnostics(self, scope=None):
        where, args = ("WHERE scope=?", (str(scope),)) if scope is not None else ("", ())
        row = self.db.execute(f"""SELECT count(*),count(scanned_at),
            sum(origin='network'),sum(origin='structural'),
            sum(dom_status='not_rendered'),
            sum(scanned_at IS NULL AND coalesce(dom_status,'')!='not_rendered'),
            sum(present_at IS NOT NULL),sum(control_at IS NOT NULL)
            FROM ui_parent_order {where}""", args).fetchone()
        values = [int(value or 0) for value in row]
        return dict(zip(("orderedParents", "scannedParents", "networkParents",
                         "structuralParents", "notRenderedParents",
                         "unresolvedParents", "domPresentParents",
                         "parentsWithControls"), values))

    def scanned_parent_ids(self, scope):
        return {str(row[0]) for row in self.db.execute(
            "SELECT parent_id FROM ui_parent_order WHERE scope=? AND scanned_at IS NOT NULL",
            (str(scope),))}

    def unscanned_parent_ids(self, scope, *, limit=None):
        query = ("SELECT parent_id FROM ui_parent_order WHERE scope=? "
                 "AND scanned_at IS NULL ORDER BY position")
        args = [str(scope)]
        if limit is not None:
            query += " LIMIT ?"
            args.append(int(limit))
        return [str(row[0]) for row in self.db.execute(query, args)]

    def scan_counts(self, scope=None):
        where, args = ("WHERE scope=?", (str(scope),)) if scope is not None else ("", ())
        row = self.db.execute(f"SELECT count(*), count(scanned_at) FROM ui_parent_order {where}",
                              args).fetchone()
        return {"loadedParents": int(row[0]), "scannedParents": int(row[1])}

    def save_thread_state(self, scope, parent_id, payload):
        self.db.execute("INSERT INTO ui_thread_state VALUES(?,?,?,?) ON CONFLICT(scope,parent_id) "
                        "DO UPDATE SET payload=excluded.payload,updated_at=excluded.updated_at",
                        (str(scope), str(parent_id), encode(payload), time.time()))

    def thread_state(self, scope, parent_id):
        row = self.db.execute("SELECT payload FROM ui_thread_state WHERE scope=? AND parent_id=?",
                              (str(scope), str(parent_id))).fetchone()
        return json.loads(row[0]) if row else None

    def add_invocation_metric(self, key, amount=1):
        metrics = self.get_meta("invocationMetrics", {})
        metrics[key] = metrics.get(key, 0) + amount
        self.set_meta("invocationMetrics", metrics)

    def add_thread_metric(self, parent_id, amount=1, *, cap=2000):
        """New replies per thread this invocation; bounded so the checkpoint
        stays small on a post with thousands of threads."""
        metrics = self.get_meta("invocationMetrics", {})
        table = metrics.get("newRepliesByThread")
        if not isinstance(table, dict):
            table = {}
        key = str(parent_id)
        if key in table or len(table) < cap:
            table[key] = table.get(key, 0) + amount
        else:
            metrics["newRepliesByThreadOverflow"] = metrics.get("newRepliesByThreadOverflow", 0) + amount
        metrics["newRepliesByThread"] = table
        self.set_meta("invocationMetrics", metrics)

    def deep_metrics(self):
        baseline = self.get_meta("invocationBaseline", {"parents": 0, "replies": 0})
        parents = self.db.execute("SELECT count(*) FROM records WHERE kind='comment' AND json_extract(payload,'$.parentCommentId') IS NULL").fetchone()[0]
        replies = self.db.execute("SELECT count(*) FROM records WHERE kind='comment' AND json_extract(payload,'$.parentCommentId') IS NOT NULL").fetchone()[0]
        confirmed = self.db.execute("""SELECT count(*) FROM jobs WHERE kind='replies'
            AND status='done' AND (json_extract(payload,'$.uiExhaustionConfirmed')=1
            OR coalesce(json_extract(payload,'$.source'),'')!='native_ui')""").fetchone()[0]
        deferred = self.db.execute("SELECT count(*) FROM jobs WHERE kind='replies' AND status!='done'").fetchone()[0]
        unproven = self.db.execute("""SELECT count(*) FROM jobs WHERE kind='replies'
            AND status='done' AND coalesce(json_extract(payload,'$.source'),'')='native_ui'
            AND coalesce(json_extract(payload,'$.uiExhaustionConfirmed'),0)!=1""").fetchone()[0]
        discovered = self.db.execute("SELECT count(*) FROM jobs WHERE kind='replies'").fetchone()[0]
        gaps = self.db.execute("""SELECT coalesce(sum(json_array_length(json_extract(payload,'$.scanGaps'))),0)
            FROM jobs WHERE kind='parents' AND json_type(json_extract(payload,'$.scanGaps'))='array'""").fetchone()[0]
        defaults = {"parentScrollSteps": 0, "acceptedParentPages": 0,
                    "uiSettleChecks": 0, "replyClicks": 0,
                    "uiCommandSeconds": 0, "replyWaitSeconds": 0,
                    "threadListSeconds": 0, "harSeconds": 0,
                    "harDumps": 0, "fullHarDumps": 0, "filteredHarDumps": 0,
                    "harEntries": 0, "harBytes": 0, "responseSourceFallbacks": 0,
                    "pageFetchReads": 0, "dateFilteredComments": 0, "chronologicalPages": 0,
                    "pagesAfterRecovery": 0,
                    "postReopens": 0, "visibleThreads": 0,
                    "rediscoveredContinuations": 0, "skippedCompletedThreads": 0,
                    "skippedKnownReplyViews": 0, "targetedParentMoves": 0,
                    "scanSteps": 0,
                    "anchorRestores": 0, "anchorFallbacks": 0,
                    "countConflicts": 0, "misattributedClicks": 0,
                    "observerReads": 0, "observerEvents": 0,
                    "observerBytes": 0, "observerReadSeconds": 0,
                    "observerAcks": 0, "recoveryReloadPages": 0,
                    "recoveryEstimateExtensions": 0,
                    "recoveryAnchorsAbsent": 0, "recoveryAnchorsEvicted": 0,
                    "gapAuditParentsScanned": 0, "gapAuditFocusMoves": 0,
                    "immediateParentChecks": 0, "immediateParentsPresent": 0,
                    "directUiQueued": 0, "directUiAttempts": 0,
                    "directUiReplies": 0, "directDomOnlyClicks": 0}
        metrics = dict(self.get_meta("invocationMetrics", {}))
        threads_with_new = metrics.pop("newRepliesByThread", {})
        scan = self.scan_counts()
        scan_detail = self.parent_scan_diagnostics()
        return {**defaults, **metrics,
                **scan, **scan_detail,
                "unscannedParents": max(0, scan["loadedParents"] - scan["scannedParents"]),
                "scanGaps": int(gaps or 0),
                "threadsDiscovered": discovered,
                "threadsWithNewReplies": len(threads_with_new) if isinstance(threads_with_new, dict) else 0,
                "newParents": max(0, parents - baseline.get("parents", 0)),
                "newReplies": max(0, replies - baseline.get("replies", 0)),
                "totalParents": parents, "totalReplies": replies,
                "confirmedThreads": confirmed, "threadsCompleted": confirmed,
                "unprovenDoneThreads": unproven, "deferredThreads": deferred}

    # Command recovery deliberately stores only a digest of request parameters.
    # Cookies, authorization headers and signed offload URLs never enter SQLite.
    def begin_operation(self, job_id, operation_kind, fingerprint, attempt=1):
        operation_id = uuid.uuid4().hex
        now = time.time()
        self.db.execute(
            "INSERT INTO command_operations VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (operation_id, job_id, None, None, operation_kind, fingerprint,
             int(attempt), "intent", "submit", None, None, now, now, None, None))
        self.db.execute("INSERT INTO command_operation_events(operation_id,state,phase,observed_at) VALUES(?,?,?,?)",
                        (operation_id, "intent", "submit", now))
        return operation_id

    def update_operation(self, operation_id, state, *, session_id=None,
                         command_id=None, phase=None, last_status=None,
                         error_type=None):
        now = time.time()
        previous = self.db.execute(
            "SELECT state,phase,last_status,error_type FROM command_operations WHERE id=?",
            (operation_id,)).fetchone()
        response_at = now if state in ("decoded", "done") else None
        self.db.execute("""UPDATE command_operations SET
            state=?, session_id=coalesce(?,session_id),
            command_id=coalesce(?,command_id), phase=coalesce(?,phase),
            last_status=coalesce(?,last_status), error_type=coalesce(?,error_type),
            updated_at=?, response_at=coalesce(?,response_at) WHERE id=?""",
            (state, session_id, command_id, phase, last_status, error_type,
             now, response_at, operation_id))
        event = (state, phase or (previous["phase"] if previous else None),
                 last_status or (previous["last_status"] if previous else None),
                 error_type or (previous["error_type"] if previous else None))
        if not previous or event != tuple(previous):
            self.db.execute("""INSERT INTO command_operation_events(
                operation_id,state,phase,last_status,error_type,observed_at)
                VALUES(?,?,?,?,?,?)""", (operation_id, *event, now))

    def save_pending_page(self, job_id, operation_id, source, payload):
        now = time.time()
        with self.transaction():
            self.db.execute("""INSERT INTO pending_pages VALUES(?,?,?,?,?,NULL)
                ON CONFLICT(job_id) DO UPDATE SET operation_id=excluded.operation_id,
                source=excluded.source,payload=excluded.payload,
                created_at=excluded.created_at,applied_at=NULL""",
                (job_id, operation_id, source, encode(payload), now))
            self.update_operation(operation_id, "decoded", phase="decode", last_status="done")

    def pending_page(self, job_id):
        row = self.db.execute(
            "SELECT * FROM pending_pages WHERE job_id=? AND applied_at IS NULL", (job_id,)).fetchone()
        return ({**dict(row), "payload": json.loads(row["payload"])}) if row else None

    def mark_pending_page_applied(self, job_id):
        row = self.db.execute(
            "SELECT operation_id FROM pending_pages WHERE job_id=? AND applied_at IS NULL", (job_id,)).fetchone()
        if not row:
            return
        now = time.time()
        self.db.execute("UPDATE pending_pages SET applied_at=? WHERE job_id=?", (now, job_id))
        if row["operation_id"]:
            self.db.execute("UPDATE command_operations SET state='applied',phase='commit',updated_at=?,applied_at=? WHERE id=?",
                            (now, now, row["operation_id"]))

    def save_observed_page(self, page_id, job_id, operation_id, source, payload):
        self.db.execute("INSERT OR IGNORE INTO observed_pages VALUES(?,?,?,?,?,?,NULL)",
                        (str(page_id), str(job_id), operation_id, source,
                         encode(payload), time.time()))

    def unapplied_observed_pages(self):
        for row in self.db.execute(
                "SELECT * FROM observed_pages WHERE applied_at IS NULL ORDER BY created_at"):
            yield {**dict(row), "payload": json.loads(row["payload"])}

    def mark_observed_page_applied(self, page_id):
        now = time.time()
        row = self.db.execute("SELECT operation_id FROM observed_pages WHERE id=?",
                              (page_id,)).fetchone()
        self.db.execute("UPDATE observed_pages SET applied_at=? WHERE id=?", (now, page_id))
        if row and row["operation_id"]:
            self.db.execute("UPDATE command_operations SET state='applied',phase='commit',"
                            "updated_at=?,applied_at=? WHERE id=?",
                            (now, now, row["operation_id"]))

    def reopen_command_failures(self, *, exclude=(), counter="reopenedAfterCommandFailure"):
        """Re-open tasks blocked by a getbro command failure (a read timeout,
        a lost payload), cursor intact. Instagram-side stops stay blocked.
        Returns the re-opened job ids."""
        reopened = []
        for row in self.db.execute("""SELECT id,payload FROM jobs WHERE status='blocked'
                AND json_extract(payload,'$.stopReason') IN ('BroCommandError','payload_download_failed')""").fetchall():
            if row['id'] in exclude:
                continue
            payload = json.loads(row['payload'])
            payload[counter] = int(payload.get(counter, 0)) + 1
            payload['lastCommandFailure'] = {'stopReason': payload.pop('stopReason', None),
                                             'commandError': payload.pop('commandError', None)}
            self.db.execute("UPDATE jobs SET status='pending',payload=? WHERE id=?",
                            (encode(payload), row['id']))
            reopened.append(row['id'])
        return reopened

    def reopen_page_fetch_timeouts(self, *, max_retries=1):
        """Give a confirmed cancelled GET one later chance after other work.

        The same cursor is retained. A second cancellation stays blocked for
        this invocation, even when other lists keep making progress.
        """
        if max_retries < 1:
            return []
        reopened = []
        for row in self.db.execute("""SELECT id,payload FROM jobs WHERE status='blocked'
                AND json_extract(payload,'$.stopReason')='PageFetchReadTimeoutError'""").fetchall():
            payload = json.loads(row['payload'])
            if int(payload.get('pageFetchRetryUsed', 0)) >= max_retries:
                continue
            payload['pageFetchRetryUsed'] = int(payload.get('pageFetchRetryUsed', 0)) + 1
            payload.pop('stopReason', None)
            self.db.execute("UPDATE jobs SET status='pending',payload=? WHERE id=?",
                            (encode(payload), row['id']))
            reopened.append(row['id'])
        return reopened

    def get_job(self, job_id):
        row = self.db.execute("SELECT * FROM jobs WHERE id=?", (str(job_id),)).fetchone()
        return {**dict(row), "payload": json.loads(row["payload"])} if row else None

    def unfinished_operation(self):
        row = self.db.execute("""SELECT * FROM command_operations
            WHERE state NOT IN ('applied','failed','deferred')
            ORDER BY updated_at DESC LIMIT 1""").fetchone()
        return dict(row) if row else None

    def command_summary(self):
        rows = dict(self.db.execute("SELECT state,count(*) FROM command_operations GROUP BY state"))
        recovery_events = self.db.execute(
            "SELECT count(*) FROM command_operation_events WHERE state='recovered'").fetchone()[0]
        deferred = self.db.execute("""SELECT count(*) FROM jobs WHERE status='blocked'
            AND json_extract(payload,'$.stopReason') IN ('BroCommandError','payload_download_failed',
                                                       'PageFetchReadTimeoutError')""").fetchone()[0]
        return {"operationsByState": rows,
                "recoveredCommands": max(self.get_meta("recoveredCommands", 0), recovery_events),
                "recoveredCommandsThisInvocation": self.get_meta(
                    "recoveredCommandsThisInvocation", 0),
                "deferredTasks": deferred,
                "pendingDecodedPages": (
                    self.db.execute("SELECT count(*) FROM pending_pages WHERE applied_at IS NULL").fetchone()[0]
                    + self.db.execute("SELECT count(*) FROM observed_pages WHERE applied_at IS NULL").fetchone()[0])}

    def coverage(self):
        pending = dict(self.db.execute("SELECT kind,count(*) FROM jobs WHERE status!='done' GROUP BY kind"))
        reasons = dict(self.db.execute("SELECT coalesce(json_extract(payload,'$.stopReason'),'pending'),count(*) FROM jobs WHERE status!='done' GROUP BY 1"))
        native_reasons = dict(self.db.execute("SELECT coalesce(json_extract(payload,'$.nativeFallbackReason'),json_extract(payload,'$.nativeObservation.status'),'not_observed'),count(*) FROM jobs WHERE kind='parents' GROUP BY 1"))
        targets_pending = self.db.execute("SELECT count(*) FROM targets WHERE status!='done'").fetchone()[0]
        parents = replies = unknown = unavailable = 0
        for row in self.db.execute("SELECT kind,payload,fields FROM records"):
            if row[0] == "comment":
                if json.loads(row[1]).get("parentCommentId"):
                    replies += 1
                else:
                    parents += 1
            for field in json.loads(row[2]).values():
                unknown += field.get("status") == "not_requested"
                unavailable += field.get("status") == "unavailable"
        scan = {**self.scan_counts(), **self.parent_scan_diagnostics()}
        scan["unscannedParents"] = max(0, scan["loadedParents"] - scan["scannedParents"])
        return {"parents": parents, "replies": replies,
                "parentScan": scan,
                "unfinishedLists": pending.get("parents", 0) + pending.get("replies", 0),
                "pendingEnrichment": pending.get("enrich", 0), "pendingByKind": pending,
                "unfinishedReasons": reasons, "nativeObservation": native_reasons,
                "fieldsNotRequested": unknown, "fieldsUnavailable": unavailable,
                "targetsNotDiscovered": targets_pending,
                "traversal": ("not_started" if not self.get_meta("collectionStarted", False) else
                              "partial" if targets_pending or any(v for k, v in pending.items() if k != "enrich") else "source_exhausted"),
                "fields": ("not_observed" if not self.count() else "pending" if pending.get("enrich") else
                           "unavailable_fields" if unknown or unavailable else "observed")}

    def close(self):
        self.db.close()


class PersistentDataset:
    def __init__(self, state, directory, fmt):
        self.state, self.directory, self.format = state, Path(directory), fmt
        self.directory.mkdir(parents=True, exist_ok=True)
        self.path = self.directory / f"items.{fmt}"
        self.last_export = time.monotonic()
        self.export_lock = threading.Lock()
        self.stop_export = threading.Event()
        self.export_thread = None

    def start_exporter(self, interval=30):
        """Keep the exported snapshot current even during a slow browser call."""
        if self.export_thread:
            return
        self.stop_export.clear()
        def export_loop():
            while not self.stop_export.wait(interval):
                try:
                    self.flush()
                except Exception as exc:  # noqa: BLE001 - the next flush retries
                    log.warning("background export of %s failed (%s); the next one retries",
                                self.path.name, type(exc).__name__)
        self.export_thread = threading.Thread(target=export_loop, daemon=True, name="dataset-export")
        self.export_thread.start()

    def stop_exporter(self):
        self.stop_export.set()
        if self.export_thread:
            self.export_thread.join()
            self.export_thread = None

    @property
    def records(self):
        return self.state.rows()

    def __len__(self):
        return self.state.count()

    def push(self, record):
        kind = "comment" if "postUrl" in record else "post"
        scope = record.get("postUrl") or record.get("inputUrl") or "dataset"
        with self.state.transaction():
            self.state.upsert(record, scope=scope, kind=kind, source=record.get("dataSource", "api"))
        self.maybe_flush()

    def maybe_flush(self):
        if time.monotonic() - self.last_export >= 30:
            self.flush()

    def flush(self):
        with self.export_lock:
            # A separate connection lets the periodic exporter run while the
            # collector is in a network request. Both CSV passes share a single
            # SQLite snapshot, so concurrent new fields cannot break its header.
            with closing(sqlite3.connect(self.state.path)) as snapshot:
                snapshot.execute("BEGIN")
                def rows():
                    for row in snapshot.execute("SELECT payload FROM records ORDER BY rowid"):
                        yield json.loads(row[0])
                return self._write_snapshot(rows)

    def _write_snapshot(self, rows):
        temporary = self.path.with_suffix(self.path.suffix + ".tmp")
        try:
            with temporary.open("w", encoding="utf-8-sig" if self.format == "csv" else "utf-8", newline="") as output:
                if self.format == "csv":
                    columns = dict.fromkeys(k for row in rows() for k in row)
                    writer = csv.DictWriter(output, fieldnames=list(columns))
                    if columns:
                        writer.writeheader()
                    for row in rows():
                        writer.writerow({k: _csv_cell(row.get(k)) for k in columns})
                else:
                    if self.format == "json":
                        output.write("[\n")
                    for i, row in enumerate(rows()):
                        if self.format == "json" and i:
                            output.write(",\n")
                        output.write(encode(row))
                        if self.format == "jsonl":
                            output.write("\n")
                    if self.format == "json":
                        output.write("\n]\n")
                output.flush()
                os.fsync(output.fileno())
            os.replace(temporary, self.path)
        finally:
            temporary.unlink(missing_ok=True)
        self.last_export = time.monotonic()
        return self.path

    close = flush


class PersistentStorage:
    def __init__(self, root, *, fmt="json", resume=False, name=None):
        self.root = Path(root)
        path = self.root / "state.sqlite"
        if resume and not path.is_file():
            raise InputError("Resume directory has no state.sqlite")
        if not resume and path.exists():
            raise InputError("State already exists; use --resume or choose a new outputDir")
        self.state = StateStore(path)
        
        from .storage import generate_unique_name
        if resume:
            self.name = self.state.get_meta("datasetName")
            if not self.name:
                self.name = name if name else "default"
        else:
            self.name = name if name is not None else generate_unique_name()
            self.state.set_meta("datasetName", self.name)
            
        self.dataset = PersistentDataset(self.state, self.root / "datasets" / self.name, fmt)
        self.kv = KeyValueStore(self.root / "key_value_stores" / self.name)

    def save_input(self, payload):
        self.kv.set("INPUT", payload)

    def save_summary(self, payload):
        return self.kv.set("OUTPUT", payload)

    def finish(self):
        self.dataset.stop_exporter()
        return self.dataset.flush()
