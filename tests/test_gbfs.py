from datetime import datetime, timedelta, timezone

import pyarrow.parquet as pq
import pytest

from bikecast import collect, gbfs
from bikecast.validate import validate_status_rows

V3_DISCOVERY = {
    "last_updated": "2026-10-21T13:00:00Z",
    "ttl": 60,
    "version": "3.0",
    "data": {
        "feeds": [
            {"name": "station_status", "url": "https://x.test/v3/station_status.json"},
            {"name": "station_information", "url": "https://x.test/v3/station_information.json"},
            {"name": "vehicle_types", "url": "https://x.test/v3/vehicle_types.json"},
        ]
    },
}
V2_DISCOVERY = {
    "last_updated": 1760000000,
    "ttl": 60,
    "version": "2.3",
    "data": {"en": {"feeds": [{"name": "station_status", "url": "https://x.test/v2/station_status.json"}]}},
}
V3_STATUS = {
    "last_updated": "2026-10-21T13:00:00Z",
    "version": "3.0",
    "data": {
        "stations": [
            {
                "station_id": "1",
                "is_installed": True,
                "is_renting": True,
                "is_returning": True,
                "last_reported": "2026-10-21T12:59:30Z",
                "num_vehicles_available": 5,
                "num_docks_available": 7,
                "vehicle_types_available": [
                    {"vehicle_type_id": "pedal", "count": 3},
                    {"vehicle_type_id": "ebike", "count": 2},
                ],
            },
            {"station_id": "2", "num_vehicles_available": 0, "num_docks_available": 12},
        ]
    },
}
V2_STATUS = {
    "last_updated": 1760000000,
    "data": {
        "stations": [
            {"station_id": "9", "num_bikes_available": 4, "num_ebikes_available": 1, "last_reported": 1759999990}
        ]
    },
}
VEHICLE_TYPES = {
    "data": {
        "vehicle_types": [
            {"vehicle_type_id": "pedal", "propulsion_type": "human"},
            {"vehicle_type_id": "ebike", "propulsion_type": "electric_assist"},
        ]
    }
}
V3_INFO = {
    "data": {
        "stations": [
            {"station_id": "1", "name": [{"text": "Schenley Plaza", "language": "en"}], "lat": 40.44, "lon": -79.95, "capacity": 12},
            {"station_id": "2", "name": [{"text": "Station Deux", "language": "fr"}], "lat": 40.45, "lon": -79.96, "capacity": 12},
        ]
    }
}


def test_parse_ts_handles_epoch_iso_and_junk():
    assert gbfs.parse_ts(1760000000) == datetime.fromtimestamp(1760000000, tz=timezone.utc)
    assert gbfs.parse_ts("2026-10-21T13:00:00Z") == datetime(2026, 10, 21, 13, tzinfo=timezone.utc)
    assert gbfs.parse_ts("2026-10-21T09:00:00-04:00") == datetime(2026, 10, 21, 13, tzinfo=timezone.utc)
    assert gbfs.parse_ts(None) is None
    assert gbfs.parse_ts("not a date") is None
    assert gbfs.parse_ts(0) is None


def test_localized_text_v2_and_v3():
    assert gbfs.localized_text("Plain") == "Plain"
    assert gbfs.localized_text([{"text": "Hi", "language": "en"}]) == "Hi"
    assert gbfs.localized_text([{"text": "Salut", "language": "fr"}]) == "Salut"
    assert gbfs.localized_text(None) is None


def test_discover_feeds_v3_and_v2():
    assert gbfs.discover_feeds(V3_DISCOVERY)["station_status"].endswith("v3/station_status.json")
    assert gbfs.discover_feeds(V2_DISCOVERY)["station_status"].endswith("v2/station_status.json")
    with pytest.raises(gbfs.FeedError):
        gbfs.discover_feeds({"data": {}})


def test_parse_status_v3_splits_ebikes_via_vehicle_types():
    ebike_ids = gbfs.parse_vehicle_types(VEHICLE_TYPES)
    assert ebike_ids == frozenset({"ebike"})
    rows = gbfs.parse_station_status(V3_STATUS, ebike_ids)
    assert rows[0]["bikes_available"] == 5
    assert rows[0]["ebikes_available"] == 2
    assert rows[0]["last_reported"] == datetime(2026, 10, 21, 12, 59, 30, tzinfo=timezone.utc)
    assert rows[1]["ebikes_available"] is None


def test_parse_status_v2():
    rows = gbfs.parse_station_status(V2_STATUS)
    assert rows[0]["bikes_available"] == 4
    assert rows[0]["ebikes_available"] == 1


def test_parse_station_information_v3_names():
    rows = gbfs.parse_station_information(V3_INFO)
    assert [r["name"] for r in rows] == ["Schenley Plaza", "Station Deux"]


def test_validate_drops_bad_rows_and_flags_staleness():
    now = datetime(2026, 10, 21, 13, 5, tzinfo=timezone.utc)
    rows = [
        {"station_id": "1", "bikes_available": 3},
        {"station_id": "1", "bikes_available": 4},  # duplicate
        {"station_id": "2", "bikes_available": -1},  # invalid
        {"station_id": None, "bikes_available": 1},  # invalid
        {"station_id": "3", "bikes_available": None},  # invalid
    ]
    clean, report = validate_status_rows(rows, now, now - timedelta(minutes=2))
    assert [r["station_id"] for r in clean] == ["1"]
    assert report["dropped_duplicates"] == 1
    assert report["dropped_invalid"] == 3
    assert report["stale"] is False

    _, stale_report = validate_status_rows(rows, now, now - timedelta(minutes=45))
    assert stale_report["stale"] is True
    _, missing_report = validate_status_rows(rows, now, None)
    assert missing_report["stale"] is True


def test_end_to_end_writes_parquet(tmp_path, monkeypatch):
    # Make the feed look fresh no matter when the test runs.
    fresh = datetime.now(timezone.utc).isoformat()
    status = {**V3_STATUS, "last_updated": fresh}
    payloads = {
        "https://x.test/gbfs.json": V3_DISCOVERY,
        "https://x.test/v3/station_status.json": status,
        "https://x.test/v3/station_information.json": V3_INFO,
        "https://x.test/v3/vehicle_types.json": VEHICLE_TYPES,
    }
    monkeypatch.setattr(gbfs, "fetch_json", lambda url, **kw: payloads[url])

    report = collect.run(tmp_path, ["https://x.test/gbfs.json"])
    assert report["rows_out"] == 2
    assert report["stale"] is False

    table = pq.read_table(report["snapshot_file"])
    assert table.num_rows == 2
    assert table.schema == collect.STATUS_SCHEMA
    assert sorted(table.column("ebikes_available").to_pylist(), key=lambda x: (x is None, x)) == [2, None]

    info_files = list((tmp_path / "station_information").glob("*.parquet"))
    assert len(info_files) == 1

    # Second run the same day must not rewrite station_information.
    first_mtime = info_files[0].stat().st_mtime_ns
    collect.run(tmp_path, ["https://x.test/gbfs.json"])
    assert info_files[0].stat().st_mtime_ns == first_mtime


def test_resolve_feeds_falls_back_to_second_url(monkeypatch):
    def fake_fetch(url, **kw):
        if "bad" in url:
            raise gbfs.FeedError("boom")
        return V2_DISCOVERY

    monkeypatch.setattr(gbfs, "fetch_json", fake_fetch)
    feeds = collect.resolve_feeds(["https://bad.test/gbfs.json", "https://good.test/gbfs.json"])
    assert "station_status" in feeds


def test_run_fails_loudly_when_all_discovery_urls_fail(tmp_path, monkeypatch):
    def always_fail(url, **kw):
        raise gbfs.FeedError("down")

    monkeypatch.setattr(gbfs, "fetch_json", always_fail)
    with pytest.raises(gbfs.FeedError):
        collect.run(tmp_path, ["https://x.test/gbfs.json"])
