import os
import sqlite3
import time
import uuid
from dataclasses import dataclass
from typing import List, Optional

from .fscache import snapshot_tree
from .ids import uuid7_str


@dataclass
class UUTConfig:
    uut_id: str
    name: str
    path: str
    last_tree_sha: Optional[str] = None
    updated_at: Optional[float] = None


class UUTStore:
    """Manages UUT configurations stored in the same sqlite DB as the job queue."""

    def __init__(self, db_path: str = "jobqueue.db", cache_dir: str = ".fscache") -> None:
        self.db_path = db_path
        self.cache_dir = cache_dir
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS uut_configs (
                    uut_id TEXT PRIMARY KEY,
                    name TEXT,
                    path TEXT,
                    last_tree_sha TEXT,
                    updated_at REAL
                )
                """
            )

    def list(self) -> List[UUTConfig]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT uut_id, name, path, last_tree_sha, updated_at FROM uut_configs ORDER BY name ASC"
            ).fetchall()
        return [UUTConfig(**dict(row)) for row in rows]

    def get(self, uut_id: str) -> Optional[UUTConfig]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT uut_id, name, path, last_tree_sha, updated_at FROM uut_configs WHERE uut_id = ?",
                (uut_id,),
            ).fetchone()
        return UUTConfig(**dict(row)) if row else None

    def add(self, name: str, path: str) -> UUTConfig:
        uut_id = uuid7_str()
        config = UUTConfig(uut_id=uut_id, name=name, path=os.path.abspath(path))
        with self._connect() as conn:
            conn.execute(
                "INSERT INTO uut_configs (uut_id, name, path) VALUES (?, ?, ?)",
                (config.uut_id, config.name, config.path),
            )
        return config

    def snapshot(self, uut_id: str) -> Optional[UUTConfig]:
        config = self.get(uut_id)
        if not config:
            return None
        tree_sha = snapshot_tree(config.path, cache_dir=self.cache_dir)
        updated_at = time.time()
        with self._connect() as conn:
            conn.execute(
                "UPDATE uut_configs SET last_tree_sha = ?, updated_at = ? WHERE uut_id = ?",
                (tree_sha, updated_at, uut_id),
            )
        return UUTConfig(
            uut_id=config.uut_id,
            name=config.name,
            path=config.path,
            last_tree_sha=tree_sha,
            updated_at=updated_at,
        )
