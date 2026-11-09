"""Fetching and parsing GBFS feeds (v2.x and v3.x).

POGOH (Pittsburgh) runs on the PBSC platform, which publishes GBFS 3.0. The
v3 spec changed several things compared with v2, and this module normalizes
both so the rest of the code never has to care:

* ``num_bikes_available``      -> ``num_vehicles_available``
* ``last_updated``/``last_reported``: epoch seconds -> ISO 8601 strings
* station ``name``: plain string -> list of ``{"text", "language"}`` objects
* ``gbfs.json`` feed list: ``data.<lang>.feeds`` -> ``data.feeds``
"""

from __future__ import annotations

import logging
import time
from datetime import UTC, datetime
from typing import Any

import requests

log = logging.getLogger(__name__)

# Feed host confirmed via public sources; the exact path is the standard PBSC
# layout but should be verified with a local run (see README).
DEFAULT_GBFS_URLS = [
    "https://pittsburgh.publicbikesystem.net/customer/gbfs/v3.0/gbfs.json",
    "https://pittsburgh.publicbikesystem.net/customer/gbfs/v2/gbfs.json",
]

USER_AGENT = "pogoh-forecast/0.1 (personal ML portfolio project)"


class FeedError(RuntimeError):
    """Raised when a feed can't be fetched or has an unexpected shape."""


def fetch_json(url: str, retries: int = 3, timeout: float = 15.0) -> dict[str, Any]:
    """GET a JSON document with simple retry and backoff."""
    last_exc: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, timeout=timeout, headers={"User-Agent": USER_AGENT})
            resp.raise_for_status()
            payload = resp.json()
            if not isinstance(payload, dict):
                raise FeedError(f"{url}: expected a JSON object")
            return payload
        except (requests.RequestException, ValueError, FeedError) as exc:
            last_exc = exc
            log.warning("fetch attempt %d/%d failed for %s: %s", attempt, retries, url, exc)
            if attempt < retries:
                time.sleep(2**attempt)
    raise FeedError(f"giving up on {url}: {last_exc}")


def parse_ts(value: Any) -> datetime | None:
    """Parse an epoch (v2) or ISO 8601 (v3) timestamp into an aware UTC datetime."""
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        if value <= 0:
            return None
        return datetime.fromtimestamp(value, tz=UTC)
    if isinstance(value, str):
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return dt.astimezone(UTC)
    return None


def localized_text(value: Any) -> str | None:
    """v2 gives a string; v3 gives [{"text": ..., "language": ...}]."""
    if isinstance(value, str):
        return value
    if isinstance(value, list) and value:
        for item in value:
            if isinstance(item, dict) and item.get("language", "").startswith("en"):
                return item.get("text")
        first = value[0]
        if isinstance(first, dict):
            return first.get("text")
    return None


def discover_feeds(gbfs_json: dict[str, Any]) -> dict[str, str]:
    """Map feed name -> URL from a gbfs.json discovery document (v2 or v3)."""
    data = gbfs_json.get("data")
    if not isinstance(data, dict):
        raise FeedError("gbfs.json has no 'data' object")
    if "feeds" in data:  # v3
        feeds = data["feeds"]
    else:  # v1/v2: keyed by language
        lang = data.get("en") or next((v for v in data.values() if isinstance(v, dict) and "feeds" in v), None)
        if lang is None:
            raise FeedError("gbfs.json has no feed list")
        feeds = lang["feeds"]
    return {f["name"]: f["url"] for f in feeds if "name" in f and "url" in f}


def parse_vehicle_types(payload: dict[str, Any]) -> frozenset[str]:
    """IDs of vehicle types that are not purely human-powered (i.e. e-bikes)."""
    types = payload.get("data", {}).get("vehicle_types", [])
    return frozenset(
        t["vehicle_type_id"]
        for t in types
        if t.get("vehicle_type_id") and t.get("propulsion_type", "human") != "human"
    )


def _opt_int(value: Any) -> int | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return int(value)


def parse_station_status(
    payload: dict[str, Any], ebike_type_ids: frozenset[str] = frozenset()
) -> list[dict[str, Any]]:
    """Normalize station_status into flat dict rows (v2 and v3)."""
    stations = payload.get("data", {}).get("stations")
    if not isinstance(stations, list):
        raise FeedError("station_status has no stations list")
    rows = []
    for s in stations:
        total = s.get("num_vehicles_available", s.get("num_bikes_available"))
        ebikes = s.get("num_ebikes_available")
        by_type = s.get("vehicle_types_available")
        if ebike_type_ids and isinstance(by_type, list):
            ebikes = sum(
                _opt_int(t.get("count")) or 0
                for t in by_type
                if t.get("vehicle_type_id") in ebike_type_ids
            )
        rows.append(
            {
                "station_id": None if s.get("station_id") is None else str(s["station_id"]),
                "bikes_available": _opt_int(total),
                "ebikes_available": _opt_int(ebikes),
                "docks_available": _opt_int(s.get("num_docks_available")),
                "is_installed": s.get("is_installed"),
                "is_renting": s.get("is_renting"),
                "is_returning": s.get("is_returning"),
                "last_reported": parse_ts(s.get("last_reported")),
            }
        )
    return rows


def parse_station_information(payload: dict[str, Any]) -> list[dict[str, Any]]:
    stations = payload.get("data", {}).get("stations")
    if not isinstance(stations, list):
        raise FeedError("station_information has no stations list")
    return [
        {
            "station_id": str(s["station_id"]),
            "name": localized_text(s.get("name")),
            "lat": s.get("lat"),
            "lon": s.get("lon"),
            "capacity": _opt_int(s.get("capacity")),
        }
        for s in stations
        if s.get("station_id") is not None
    ]
