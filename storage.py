from __future__ import annotations

import json
import sqlite3
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, Optional

DB_SCHEMA = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS cache (
    key TEXT PRIMARY KEY,
    value_json TEXT NOT NULL,
    fetched_at INTEGER NOT NULL,
    ttl_seconds INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS snapshots (
    snapshot_id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at INTEGER NOT NULL,
    snapshot_type TEXT NOT NULL,
    country TEXT NOT NULL,
    params_json TEXT NOT NULL,
    rows_json TEXT NOT NULL
);
"""


@dataclass(frozen=True)
class CacheEntry:
    value: Dict[str, Any]
    fetched_at: int
    ttl_seconds: int

    def is_expired(self) -> bool:
        return time.time() > self.fetched_at + self.ttl_seconds


class Storage:
    def __init__(self, db_path: str = "steam_radar.sqlite") -> None:
        self.db_path = db_path
        self._init_db()

    # -------------------------
    # DB plumbing
    # -------------------------
    def _connect(self) -> sqlite3.Connection:
        con = sqlite3.connect(self.db_path, check_same_thread=False)
        con.row_factory = sqlite3.Row
        return con

    def _init_db(self) -> None:
        con = self._connect()
        try:
            con.executescript(DB_SCHEMA)
            con.commit()
        finally:
            con.close()

    # -------------------------
    # Cache (low-level)
    # -------------------------
    def cache_get(self, key: str) -> Optional[CacheEntry]:
        con = self._connect()
        try:
            row = con.execute(
                "SELECT value_json, fetched_at, ttl_seconds FROM cache WHERE key=?",
                (key,),
            ).fetchone()
            if not row:
                return None

            entry = CacheEntry(
                value=json.loads(row["value_json"]),
                fetched_at=row["fetched_at"],
                ttl_seconds=row["ttl_seconds"],
            )
            if entry.is_expired():
                return None
            return entry
        finally:
            con.close()

    def cache_set(self, key: str, value: Any, ttl_seconds: int = 24 * 3600) -> None:
        con = self._connect()
        try:
            con.execute(
                """
                INSERT INTO cache (key, value_json, fetched_at, ttl_seconds)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET
                    value_json=excluded.value_json,
                    fetched_at=excluded.fetched_at,
                    ttl_seconds=excluded.ttl_seconds
                """,
                (
                    key,
                    json.dumps(value),
                    int(time.time()),
                    int(ttl_seconds),
                ),
            )
            con.commit()
        finally:
            con.close()

    # -------------------------
    # Compatibility wrappers
    # (used by steam_sources.py)
    # -------------------------
    def get_json(self, key: str):
        """
        Return cached JSON value or None.
        """
        entry = self.cache_get(key)
        return entry.value if entry else None

    def set_json(self, key: str, value: Any, ttl_seconds: int = 24 * 3600):
        """
        Store JSON-serializable value with TTL.
        """
        self.cache_set(key, value, ttl_seconds)
        return value

    # -------------------------
    # Snapshots (optional use)
    # -------------------------
    def save_snapshot(
        self,
        snapshot_type: str,
        country: str,
        params: Dict[str, Any],
        rows: Iterable[Dict[str, Any]],
    ) -> None:
        con = self._connect()
        try:
            con.execute(
                """
                INSERT INTO snapshots
                (created_at, snapshot_type, country, params_json, rows_json)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    int(time.time()),
                    snapshot_type,
                    country,
                    json.dumps(params),
                    json.dumps(list(rows)),
                ),
            )
            con.commit()
        finally:
            con.close()
