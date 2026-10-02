"""Apify-shaped local storage: a dataset plus a key-value store.

Layout under ``--output-dir`` (default ``storage/``)::

    storage/
      datasets/<dataset_name>/
        items.json       # or items.jsonl / items.csv
      key_value_stores/<dataset_name>/
        INPUT.json
        OUTPUT.json      # run summary: counts, billing, failures

JSONL records stream to disk as they arrive. JSON/CSV remain buffered until
finish; the runner also calls finish on a caught interruption or fatal error.
"""

from __future__ import annotations

import csv
import json
import logging
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

log = logging.getLogger(__name__)

__all__ = ["Dataset", "KeyValueStore", "RunStorage", "generate_unique_name"]


def generate_unique_name(prefix: str = "") -> str:
    """Generate a unique filesystem-safe name for a run.
    Format: [prefix_]YYYYMMDD_HHMMSS_8hex (e.g. 20261002_103000_a1b2c3d4)
    """
    now = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    short_uuid = uuid.uuid4().hex[:8]
    if prefix:
        return f"{prefix}_{now}_{short_uuid}"
    return f"{now}_{short_uuid}"


class Dataset:
    """Append-only record sink that can render JSON, JSONL or CSV."""

    def __init__(self, directory: Path, *, fmt: str = "json", name: str = "items") -> None:
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.format = fmt
        self.path = self.directory / f"{name}.{ 'jsonl' if fmt == 'jsonl' else fmt }"
        self.records: list[dict[str, Any]] = []
        self._lock = threading.Lock()
        self._stream = None

        if fmt == "jsonl":
            self._stream = self.path.open("w", encoding="utf-8", newline="\n")

    # ---------------------------------------------------------------- write --

    def push(self, record: dict[str, Any]) -> None:
        """Add one record."""
        with self._lock:
            self.records.append(record)
            if self._stream is not None:
                self._stream.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")
                self._stream.flush()

    def extend(self, records: Iterable[dict[str, Any]]) -> int:
        count = 0
        for record in records:
            self.push(record)
            count += 1
        return count

    def __len__(self) -> int:
        return len(self.records)

    # ----------------------------------------------------------------- read --

    def flush(self) -> Path:
        """Write the dataset out and return the file path."""
        with self._lock:
            if self.format == "jsonl":
                if self._stream is not None:
                    self._stream.flush()
                return self.path
            if self.format == "csv":
                self._write_csv()
                return self.path
            self.path.write_text(
                json.dumps(self.records, ensure_ascii=False, indent=2, default=str),
                encoding="utf-8",
            )
            return self.path

    def close(self) -> Path:
        path = self.flush()
        if self._stream is not None:
            self._stream.close()
            self._stream = None
        return path

    def _write_csv(self) -> None:
        """Flatten nested values so a spreadsheet can open the result."""
        if not self.records:
            self.path.write_text("", encoding="utf-8")
            return
        columns: list[str] = []
        for record in self.records:
            for key in record:
                if key not in columns:
                    columns.append(key)
        with self.path.open("w", encoding="utf-8-sig", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=columns, extrasaction="ignore")
            writer.writeheader()
            for record in self.records:
                writer.writerow({key: _csv_cell(record.get(key)) for key in columns})


def _csv_cell(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, list) and all(isinstance(v, (str, int, float)) for v in value):
        return ", ".join(str(v) for v in value)
    return json.dumps(value, ensure_ascii=False, default=str)


class KeyValueStore:
    """Files keyed by name, mirroring Apify's default key-value store."""

    def __init__(self, directory: Path) -> None:
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)

    def set(self, key: str, value: Any) -> Path:
        path = self.directory / (key if key.endswith(".json") else f"{key}.json")
        path.write_text(
            json.dumps(value, ensure_ascii=False, indent=2, default=str), encoding="utf-8"
        )
        return path

    def get(self, key: str, default: Any = None) -> Any:
        path = self.directory / (key if key.endswith(".json") else f"{key}.json")
        if not path.exists():
            return default
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return default


class RunStorage:
    """Dataset + key-value store for one run."""

    def __init__(self, root: Path, *, fmt: str = "json", name: str | None = None) -> None:
        self.root = Path(root)
        self.name = name if name is not None else generate_unique_name()
        self.dataset = Dataset(self.root / "datasets" / self.name, fmt=fmt)
        self.kv = KeyValueStore(self.root / "key_value_stores" / self.name)
        self.started_at = datetime.now(timezone.utc)

    def save_input(self, payload: dict[str, Any]) -> None:
        self.kv.set("INPUT", payload)

    def save_summary(self, payload: dict[str, Any]) -> Path:
        finished = datetime.now(timezone.utc)
        summary = {
            "startedAt": self.started_at.isoformat(),
            "finishedAt": finished.isoformat(),
            "durationSeconds": round((finished - self.started_at).total_seconds(), 2),
            **payload,
        }
        return self.kv.set("OUTPUT", summary)

    def finish(self) -> Path:
        path = self.dataset.close()
        log.info("wrote %d records to %s", len(self.dataset), path)
        return path
