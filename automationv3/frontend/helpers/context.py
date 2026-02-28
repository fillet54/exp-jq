"""Shared dependency container for frontend helper functions."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from flask import Flask


@dataclass(frozen=True)
class FrontendHelperContext:
    """Runtime dependencies needed by frontend helper functions."""

    app: Flask
    queue: Any
    reporting: Any
    central: Any
    uut_store: Any
    suite_manager: Any
    scripts_root: Path
    scripts_cache_dir: str
    log: logging.Logger
