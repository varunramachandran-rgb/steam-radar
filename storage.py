from __future__ import annotations

import json
import sqlite3
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional


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


class Storage:
    def __init__(self, db_path: str = "steam_radar.sqlite") -> None:
        self.db_path = db_path
        self._init_db()

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

    # ---------- JSON cache helpers ----------
    def get_json(self, key: str) -> Optional[Dict[str, Any]]:
        """Get cached JSON value if present and not expired."""
        con = self._connect()
        try:
            row = con.execute(
                "SELECT value_json, fetched_at, ttl_seconds FROM cache WHERE key=?",
                (key,),
            ).fetchone()
            if not row:
                return None

            fetched_at = int(row["fetched_at"])
            ttl_seconds = int(row["ttl_seconds"])
            now = int(time.time())
            if now - fetched_at > ttl_seconds:
                con.execute("DELETE FROM cache WHERE key=?", (key,))
                con.commit()
                return None

            return json.loads(row["value_json"])
        finally:
            con.close()

    def set_json(self, key: str, value: Dict[str, Any], ttl_seconds: int) -> None:
        con = self._connect()
        try:
            con.execute(
                """
                INSERT INTO cache(key, value_json, fetched_at, ttl_seconds)
                VALUES(?, ?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET
                  value_json=excluded.value_json,
                  fetched_at=excluded.fetched_at,
                  ttl_seconds=excluded.ttl_seconds
                """,
                (key, json.dumps(value), int(time.time()), int(ttl_seconds)),
            )
            con.commit()
        finally:
            con.close()

    # ---------- Snapshot helpers ----------
    def save_snapshot(self, snapshot_type: str, country: str, params: Dict[str, Any], rows: list[Dict[str, Any]]) -> None:
        con = self._connect()
        try:
            con.execute(
                """
                INSERT INTO snapshots(created_at, snapshot_type, country, params_json, rows_json)
                VALUES(?, ?, ?, ?, ?)
                """,
                (
                    int(time.time()),
                    snapshot_type,
                    country,
                    json.dumps(params),
                    json.dumps(rows),
                ),
            )
            con.commit()
        finally:
            con.close()
