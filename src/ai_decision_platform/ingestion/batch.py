"""Ingestion batch quotidienne : archives sources et tables Bronze Parquet."""

import argparse
import hashlib
import json
import time
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import urlopen
from uuid import uuid4

import holidays
import polars as pl

from ai_decision_platform.config import load_settings
from ai_decision_platform.logging_config import configure_logging

RTE = "https://odre.opendatasoft.com/api/explore/v2.1/catalog/datasets/eco2mix-national-cons-def/records"
WEATHER = "https://archive-api.open-meteo.com/v1/archive"


def fetch_json(url: str) -> dict:
    """Trois tentatives pour erreurs transitoires, timeout explicite, TLS vérifié."""
    for attempt in range(3):
        try:
            with urlopen(url, timeout=45) as response:
                return json.load(response)
        except HTTPError as exc:
            if exc.code not in {429, 500, 502, 503, 504} or attempt == 2:
                raise
        except (URLError, TimeoutError):
            if attempt == 2:
                raise
        time.sleep(2**attempt)
    raise RuntimeError("Téléchargement impossible")


def energy_for_day(day: date, fetch=fetch_json) -> tuple[pl.DataFrame, list, list]:
    end = day + timedelta(days=1)
    where = (
        f"date_heure >= date'{day.isoformat()}T00:00:00Z' AND "
        f"date_heure < date'{end.isoformat()}T00:00:00Z'"
    )
    rows, pages, urls = [], [], []
    offset, total = 0, None
    while total is None or offset < total:
        url = (
            RTE
            + "?"
            + urlencode(
                {
                    "where": where,
                    "order_by": "date_heure",
                    "limit": 100,
                    "offset": offset,
                }
            )
        )
        payload = fetch(url)
        batch = payload["results"]
        count = payload["total_count"]
        if not isinstance(count, int) or count < 0:
            raise ValueError("RTE : total_count invalide")
        if total is not None and count != total:
            raise ValueError("RTE : source modifiée pendant la pagination ; relancer")
        total = count
        if not batch:
            raise ValueError("RTE : réponse vide ou pagination incomplète")
        rows.extend(batch)
        pages.append(payload)
        urls.append(url)
        offset += len(batch)
    frame = pl.from_dicts(rows, infer_schema_length=None)
    required = {"date_heure", "consommation"}
    if not required.issubset(frame.columns):
        raise ValueError("RTE : colonnes obligatoires absentes")
    timestamps = frame["date_heure"].to_list()
    if len(set(timestamps)) != len(timestamps):
        raise ValueError("RTE : timestamps dupliqués")
    for stamp in timestamps:
        parsed = datetime.fromisoformat(stamp)
        if parsed.tzinfo is None or parsed.astimezone(UTC).date() != day:
            raise ValueError("RTE : timestamp hors de la journée UTC")
    if len(rows) != total:
        raise ValueError("RTE : nombre de lignes incohérent")
    return frame, pages, urls


def weather_for_day(day: date, latitude: float, longitude: float, fetch=fetch_json):
    url = (
        WEATHER
        + "?"
        + urlencode(
            {
                "latitude": latitude,
                "longitude": longitude,
                "start_date": day.isoformat(),
                "end_date": day.isoformat(),
                "hourly": "temperature_2m",
                "timezone": "UTC",
                "models": "era5",
            }
        )
    )
    payload = fetch(url)
    hourly = payload["hourly"]
    stamps, temperatures = hourly["time"], hourly["temperature_2m"]
    expected = [f"{day.isoformat()}T{hour:02d}:00" for hour in range(24)]
    if stamps != expected or len(temperatures) != 24:
        raise ValueError("Météo : 24 heures UTC attendues dans l'ordre")
    if payload.get("utc_offset_seconds") != 0:
        raise ValueError("Météo : timezone UTC attendue")
    if payload["hourly_units"]["temperature_2m"] != "°C":
        raise ValueError("Météo : température attendue en degrés Celsius")
    frame = pl.DataFrame(
        {
            "timestamp_utc": [stamp + "Z" for stamp in stamps],
            "temperature_2m_c": pl.Series(temperatures, dtype=pl.Float64),
            "requested_latitude": [latitude] * 24,
            "requested_longitude": [longitude] * 24,
        }
    )
    return frame, payload, [url]


def calendar_for_day(day: date) -> pl.DataFrame:
    name = holidays.country_holidays("FR", years=day.year, language="fr").get(day)
    return pl.DataFrame(
        {
            "date": [day],
            "weekday_iso": [day.isoweekday()],
            "is_weekend": [day.weekday() >= 5],
            "is_public_holiday": [name is not None],
            "holiday_name": pl.Series([name], dtype=pl.String),
        }
    )


def ingest(
    start: date, end: date, data_dir: Path, latitude=48.8566, longitude=2.3522, fetch=fetch_json
) -> Path:
    if end < start or (end - start).days >= 31:
        raise ValueError("Choisir une période de 1 à 31 jours inclus")
    if not (-90 <= latitude <= 90 and -180 <= longitude <= 180):
        raise ValueError("Coordonnées géographiques invalides")
    run_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%S") + "-" + uuid4().hex[:8]
    run_dir = data_dir / "bronze" / "runs" / run_id
    run_dir.mkdir(parents=True, exist_ok=False)
    manifest = {
        "run_id": run_id,
        "status": "running",
        "started_at": datetime.now(UTC).isoformat(),
        "start": str(start),
        "end": str(end),
        "partition_timezone": "UTC",
        "latitude": latitude,
        "longitude": longitude,
        "artifacts": [],
    }
    logger = configure_logging(load_settings().log_level)
    try:
        day = start
        while day <= end:
            energy, raw_energy, energy_urls = energy_for_day(day, fetch)
            weather, raw_weather, weather_urls = weather_for_day(day, latitude, longitude, fetch)
            calendar = calendar_for_day(day)
            for source, frame, raw, urls in [
                ("rte", energy, raw_energy, energy_urls),
                ("weather", weather, raw_weather, weather_urls),
                ("calendar", calendar, None, ["python-holidays:FR"]),
            ]:
                folder = run_dir / source / f"date={day}"
                folder.mkdir(parents=True)
                if raw is not None:
                    (folder / "response.json").write_text(
                        json.dumps(raw, ensure_ascii=False),
                        encoding="utf-8",
                    )
                path = folder / "data.parquet"
                frame.write_parquet(path)
                manifest["artifacts"].append(
                    {
                        "source": source,
                        "date": str(day),
                        "rows": frame.height,
                        "file": str(path.relative_to(run_dir)),
                        "urls": urls,
                        "null_counts": frame.null_count().to_dicts()[0],
                        "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                    }
                )
            logger.info(
                "Journée %s : RTE=%s, météo=%s, calendrier=1", day, energy.height, weather.height
            )
            day += timedelta(days=1)
        manifest["status"] = "success"
    except Exception as exc:
        manifest["status"] = "failed"
        manifest["error"] = f"{type(exc).__name__}: {exc}"
        raise
    finally:
        manifest["finished_at"] = datetime.now(UTC).isoformat()
        temporary = run_dir / "manifest.tmp"
        temporary.write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")
        temporary.replace(run_dir / "manifest.json")
    return run_dir


def main():
    parser = argparse.ArgumentParser(description="Ingestion Bronze RTE + météo + calendrier")
    parser.add_argument("--start", required=True, type=date.fromisoformat)
    parser.add_argument("--end", required=True, type=date.fromisoformat)
    parser.add_argument("--latitude", type=float, default=48.8566)
    parser.add_argument("--longitude", type=float, default=2.3522)
    args = parser.parse_args()
    path = ingest(args.start, args.end, load_settings().data_dir, args.latitude, args.longitude)
    print(path)


if __name__ == "__main__":
    main()
