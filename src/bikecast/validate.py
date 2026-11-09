"""Row-level validation for station_status snapshots."""

from __future__ import annotations

from datetime import datetime
from typing import Any

# The feed's own timestamp often lags wall-clock time. Anything older than
# this is flagged (not dropped) so downstream code can decide what to do.
DEFAULT_MAX_AGE_SECONDS = 15 * 60


def validate_status_rows(
    rows: list[dict[str, Any]],
    fetched_at: datetime,
    feed_last_updated: datetime | None,
    max_age_seconds: int = DEFAULT_MAX_AGE_SECONDS,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Drop invalid and duplicate rows and report on feed freshness.

    Returns ``(clean_rows, report)``. Staleness is reported, never silently
    "fixed": a stale snapshot is still data, and the flag lets us model it.
    """
    seen: set[str] = set()
    clean: list[dict[str, Any]] = []
    invalid = 0
    duplicates = 0
    for row in rows:
        sid = row.get("station_id")
        bikes = row.get("bikes_available")
        if not sid or bikes is None or bikes < 0:
            invalid += 1
            continue
        if sid in seen:
            duplicates += 1
            continue
        seen.add(sid)
        clean.append(row)

    age = None if feed_last_updated is None else (fetched_at - feed_last_updated).total_seconds()
    stale = age is None or age > max_age_seconds
    report = {
        "rows_in": len(rows),
        "rows_out": len(clean),
        "dropped_invalid": invalid,
        "dropped_duplicates": duplicates,
        "feed_age_seconds": age,
        "stale": stale,
    }
    return clean, report
