"""UUID helpers, including a Python 3.10-compatible UUIDv7."""

import secrets
import time
import uuid


def uuid7() -> uuid.UUID:
    """Generate a time-ordered UUIDv7 (sortable by creation time)."""
    ts_ms = int(time.time() * 1000)
    if ts_ms >= 1 << 60:
        raise ValueError("Timestamp too large for UUIDv7")

    # Break 60-bit timestamp into UUID time fields (big-endian order)
    time_low = (ts_ms >> 28) & 0xFFFFFFFF
    time_mid = (ts_ms >> 12) & 0xFFFF
    time_hi = ts_ms & 0x0FFF
    time_hi_and_version = time_hi | (0x7 << 12)

    # 62 bits of randomness split into clock seq (14b) and node (48b)
    rand62 = secrets.randbits(62)
    clock_seq = rand62 & 0x3FFF
    node = rand62 >> 14
    clock_seq_low = clock_seq & 0xFF
    clock_seq_hi_and_reserved = (clock_seq >> 8) & 0x3F
    clock_seq_hi_and_reserved |= 0x80  # RFC 4122 variant (10xx)

    return uuid.UUID(
        fields=(
            time_low,
            time_mid,
            time_hi_and_version,
            clock_seq_hi_and_reserved,
            clock_seq_low,
            node,
        )
    )


def uuid7_str() -> str:
    """Return UUIDv7 as a string."""
    return str(uuid7())


__all__ = ["uuid7", "uuid7_str"]
