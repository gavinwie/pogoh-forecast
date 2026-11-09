"""Collect one station_status snapshot and write it as a small Parquet file.

Usage:
    python -m bikecast.collect --out ./data
    python -m bikecast.collect --out ./data --url https://.../gbfs.json

Layout written under --out:
    raw/YYYY-MM-DD/HHMMSS.parquet           one file per snapshot (UTC)
    station_information/YYYY-MM-DD.parquet  static station metadata, once a day
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from bikecast import gbfs
from bikecast.validate import DEFAULT_MAX_AGE_SECONDS, validate_status_rows

log = logging.getLogger("bikecast.collect")

STATUS_SCHEMA = pa.schema(
    [
        ("snapshot_ts", pa.timestamp("us", tz="UTC")),
        ("feed_last_updated", pa.timestamp("us", tz="UTC")),
        ("station_id", pa.string()),
        ("bikes_available", pa.int32()),
        ("ebikes_available", pa.int32()),
        ("docks_available", pa.int32()),
        ("is_installed", pa.bool_()),
        ("is_renting", pa.bool_()),
        ("is_returning", pa.bool_()),
        ("last_reported", pa.timestamp("us", tz="UTC")),
        ("feed_stale", pa.bool_()),
    ]
)

INFO_SCHEMA = pa.schema(
    [
        ("station_id", pa.string()),
        ("name", pa.string()),
        ("lat", pa.float64()),
        ("lon", pa.float64()),
        ("capacity", pa.int32()),
    ]
)


def _write_parquet(table: pa.Table, path: Path) -> None:
    """Write atomically so a killed run never leaves a half-written file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    pq.write_table(table, tmp, compression="zstd")
    os.replace(tmp, path)


def resolve_feeds(urls: list[str]) -> dict[str, str]:
    """Try each discovery URL in order and return the first feed map that works."""
    errors = []
    for url in urls:
        try:
            feeds = gbfs.discover_feeds(gbfs.fetch_json(url))
            if "station_status" in feeds:
                log.info("using discovery document %s", url)
                return feeds
            errors.append(f"{url}: no station_status feed listed")
        except gbfs.FeedError as exc:
            errors.append(str(exc))
    raise gbfs.FeedError("no usable GBFS discovery document: " + " | ".join(errors))


def run(out_dir: Path, urls: list[str] | None = None, max_age_seconds: int = DEFAULT_MAX_AGE_SECONDS) -> dict[str, Any]:
    urls = urls or gbfs.DEFAULT_GBFS_URLS
    fetched_at = datetime.now(UTC)

    feeds = resolve_feeds(urls)

    ebike_ids: frozenset[str] = frozenset()
    if "vehicle_types" in feeds:
        try:
            ebike_ids = gbfs.parse_vehicle_types(gbfs.fetch_json(feeds["vehicle_types"]))
        except gbfs.FeedError as exc:  # optional enrichment; never block collection
            log.warning("vehicle_types unavailable: %s", exc)

    status = gbfs.fetch_json(feeds["station_status"])
    feed_updated = gbfs.parse_ts(status.get("last_updated"))
    rows = gbfs.parse_station_status(status, ebike_ids)
    clean, report = validate_status_rows(rows, fetched_at, feed_updated, max_age_seconds)
    if not clean:
        raise gbfs.FeedError(f"snapshot had no valid rows: {report}")

    for r in clean:
        r["snapshot_ts"] = fetched_at
        r["feed_last_updated"] = feed_updated
        r["feed_stale"] = report["stale"]
    table = pa.Table.from_pylist(clean, schema=STATUS_SCHEMA)
    snap_path = out_dir / "raw" / fetched_at.strftime("%Y-%m-%d") / f"{fetched_at.strftime('%H%M%S')}.parquet"
    _write_parquet(table, snap_path)

    info_path = out_dir / "station_information" / f"{fetched_at.strftime('%Y-%m-%d')}.parquet"
    if not info_path.exists() and "station_information" in feeds:
        try:
            info_rows = gbfs.parse_station_information(gbfs.fetch_json(feeds["station_information"]))
            _write_parquet(pa.Table.from_pylist(info_rows, schema=INFO_SCHEMA), info_path)
            report["wrote_station_information"] = True
        except gbfs.FeedError as exc:
            log.warning("station_information unavailable: %s", exc)

    report["snapshot_file"] = str(snap_path)
    report["snapshot_ts"] = fetched_at.isoformat()
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--out", type=Path, required=True, help="output directory")
    parser.add_argument("--url", action="append", help="gbfs.json URL (repeatable); defaults to POGOH")
    parser.add_argument("--max-age", type=int, default=DEFAULT_MAX_AGE_SECONDS, help="stale threshold in seconds")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    try:
        report = run(args.out, args.url, args.max_age)
    except gbfs.FeedError as exc:
        log.error("collection failed: %s", exc)
        return 1
    print(json.dumps(report, indent=2, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
