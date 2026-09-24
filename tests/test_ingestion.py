import json
from datetime import date
from urllib.error import HTTPError
from urllib.parse import parse_qs, urlsplit

import polars as pl
import pytest

from ai_decision_platform.ingestion.batch import (
    calendar_for_day,
    energy_for_day,
    fetch_json,
    ingest,
    weather_for_day,
)

DAY = date(2024, 1, 1)


def fake_fetch(url):
    if "open-meteo" in url:
        return {
            "utc_offset_seconds": 0,
            "hourly_units": {"temperature_2m": "°C"},
            "hourly": {
                "time": [f"2024-01-01T{h:02d}:00" for h in range(24)],
                "temperature_2m": [5.0] * 24,
            },
        }
    return {
        "total_count": 1,
        "results": [
            {"date_heure": "2024-01-01T00:00:00+00:00", "consommation": 50000},
        ],
    }


def test_complete_run_and_replay_are_isolated(tmp_path):
    first = ingest(DAY, DAY, tmp_path, fetch=fake_fetch)
    second = ingest(DAY, DAY, tmp_path, fetch=fake_fetch)
    assert first != second
    manifest = json.loads((first / "manifest.json").read_text())
    assert manifest["status"] == "success"
    assert [x["rows"] for x in manifest["artifacts"]] == [1, 24, 1]
    assert len(list(first.rglob("response.json"))) == 2
    assert pl.read_parquet(first / manifest["artifacts"][0]["file"])["consommation"][0] == 50000


def test_failure_has_no_success_manifest(tmp_path):
    def broken(url):
        raise ValueError("source unavailable")

    with pytest.raises(ValueError):
        ingest(DAY, DAY, tmp_path, fetch=broken)
    manifest = next(tmp_path.rglob("manifest.json"))
    assert json.loads(manifest.read_text())["status"] == "failed"


def test_pagination_uses_offsets():
    offsets = []

    def paginated(url):
        offset = int(parse_qs(urlsplit(url).query)["offset"][0])
        offsets.append(offset)
        return {
            "total_count": 2,
            "results": [
                {
                    "date_heure": f"2024-01-01T0{offset}:00:00+00:00",
                    "consommation": None,
                }
            ],
        }

    frame, _, _ = energy_for_day(DAY, paginated)
    assert offsets == [0, 1]
    assert frame.height == 2
    assert frame["consommation"].null_count() == 2


def test_empty_source_rejected():
    with pytest.raises(ValueError, match="vide"):
        energy_for_day(DAY, lambda _: {"total_count": 0, "results": []})


def test_duplicate_timestamps_rejected():
    payload = fake_fetch("rte")
    payload["results"] *= 2
    payload["total_count"] = 2
    with pytest.raises(ValueError, match="dupliqués"):
        energy_for_day(DAY, lambda _: payload)


def test_missing_weather_hour_rejected():
    payload = fake_fetch("open-meteo")
    payload["hourly"]["time"].pop()
    with pytest.raises(ValueError, match="24 heures"):
        weather_for_day(DAY, 48.8, 2.3, lambda _: payload)


def test_new_year_is_french_public_holiday():
    assert calendar_for_day(DAY)["is_public_holiday"][0]
    assert not calendar_for_day(date(2024, 1, 2))["is_public_holiday"][0]


def test_invalid_range_makes_no_output(tmp_path):
    with pytest.raises(ValueError):
        ingest(DAY, date(2023, 12, 31), tmp_path, fetch=fake_fetch)
    assert list(tmp_path.iterdir()) == []


def test_transient_http_failure_retried(monkeypatch):
    attempts = []

    def unavailable(*args, **kwargs):
        attempts.append(1)
        raise HTTPError("https://example.org", 503, "Unavailable", {}, None)

    monkeypatch.setattr("ai_decision_platform.ingestion.batch.urlopen", unavailable)
    monkeypatch.setattr("ai_decision_platform.ingestion.batch.time.sleep", lambda _: None)
    with pytest.raises(HTTPError):
        fetch_json("https://example.org")
    assert len(attempts) == 3
