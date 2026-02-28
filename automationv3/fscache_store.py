"""Filesystem cache store facade.

Provides a narrow interface around the existing fscache implementation so
higher-level modules depend on a stable store API.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from automationv3.jobqueue.fscache import calculate_sha1, snapshot_tree


@dataclass
class FSCacheStore:
    """Small wrapper around fscache snapshot/hash functions."""

    default_cache_dir: Optional[str] = None

    def snapshot_tree(self, rootdir: str, cache_dir: Optional[str] = None):
        effective_cache_dir = cache_dir or self.default_cache_dir
        if effective_cache_dir:
            return snapshot_tree(rootdir, cache_dir=effective_cache_dir)
        return snapshot_tree(rootdir)

    def calculate_sha1(self, path: str) -> str:
        return str(calculate_sha1(path))


__all__ = ["FSCacheStore", "snapshot_tree", "calculate_sha1"]
