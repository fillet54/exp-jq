"""Backward-compatible UUT exports.

Primary UUT ownership now lives under ``automationv3.reporting.uut``.
"""

from automationv3.reporting.uut import UUTConfig, UUTStore

__all__ = ["UUTStore", "UUTConfig"]
