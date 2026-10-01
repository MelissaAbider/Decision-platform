"""Recuperation d'un historique par lots mensuels, avec reprise des lots reussis."""

from __future__ import annotations

import argparse
import calendar
import hashlib
import json
from datetime import date, timedelta
from pathlib import Path

from ai_decision_platform.config import load_settings
from ai_decision_platform.ingestion.batch import ingest


def monthly_windows(start: date, end: date) -> list[tuple[date, date]]:
    """Dates inclusives ; chaque lot reste dans la limite de 31 jours de ingest."""
    if end < start:
        raise ValueError("La date de fin doit etre apres ou egale a la date de debut")
    windows = []
    cursor = start
    while cursor <= end:
        last_day = calendar.monthrange(cursor.year, cursor.month)[1]
        window_end = min(end, date(cursor.year, cursor.month, last_day))
        windows.append((cursor, window_end))
        cursor = window_end + timedelta(days=1)
    return windows


def reusable_run(
    data_dir: Path, start: date, end: date, latitude: float, longitude: float
) -> Path | None:
    """Reutiliser seulement un lot complet, aux memes dates et coordonnees."""
    expected = {
        (source, str(start + timedelta(days=offset)))
        for offset in range((end - start).days + 1)
        for source in ("rte", "weather", "calendar")
    }
    for run_dir in sorted((data_dir / "bronze" / "runs").glob("*"), reverse=True):
        manifest_path = run_dir / "manifest.json"
        if not manifest_path.exists():
            continue
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if (
            manifest.get("status") != "success"
            or manifest.get("start") != str(start)
            or manifest.get("end") != str(end)
            or manifest.get("latitude") != latitude
            or manifest.get("longitude") != longitude
        ):
            continue
        artifacts = manifest.get("artifacts", [])
        if {(item["source"], item["date"]) for item in artifacts} != expected:
            continue
        intact = True
        for item in artifacts:
            path = run_dir / item["file"]
            if (
                not path.is_file()
                or hashlib.sha256(path.read_bytes()).hexdigest() != item["sha256"]
            ):
                intact = False
                break
        if intact:
            return run_dir
    return None


def backfill(
    start: date,
    end: date,
    data_dir: Path,
    latitude: float = 48.8566,
    longitude: float = 2.3522,
    refresh: bool = False,
) -> list[Path]:
    """Un echec bloque la suite ; une relance reprend les lots mensuels reussis."""
    runs = []
    for window_start, window_end in monthly_windows(start, end):
        run = (
            None
            if refresh
            else reusable_run(data_dir, window_start, window_end, latitude, longitude)
        )
        if run is None:
            print(f"Telechargement : {window_start} -> {window_end}", flush=True)
            run = ingest(window_start, window_end, data_dir, latitude, longitude)
        else:
            print(f"Lot deja reussi : {window_start} -> {window_end} ({run.name})", flush=True)
        runs.append(run)
    return runs


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingestion historique en lots mensuels")
    parser.add_argument("--start", required=True, type=date.fromisoformat)
    parser.add_argument("--end", required=True, type=date.fromisoformat)
    parser.add_argument("--latitude", type=float, default=48.8566)
    parser.add_argument("--longitude", type=float, default=2.3522)
    parser.add_argument(
        "--refresh", action="store_true", help="Reinterroger les API meme si lot reussi"
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Afficher les lots sans telechargement"
    )
    args = parser.parse_args()
    if args.dry_run:
        for start, end in monthly_windows(args.start, args.end):
            print(f"{start} -> {end}")
        return
    runs = backfill(
        args.start,
        args.end,
        load_settings().data_dir,
        args.latitude,
        args.longitude,
        args.refresh,
    )
    print(f"Historique Bronze pret : {len(runs)} lot(s)")


if __name__ == "__main__":
    main()
