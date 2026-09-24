"""Configuration explicite, sans accès réseau ni création de dossiers."""

import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    environment: str
    log_level: str
    data_dir: Path


def load_settings(environ: Mapping[str, str] | None = None) -> Settings:
    values = os.environ if environ is None else environ
    environment = values.get("ADP_ENV", "local").strip().lower()
    if environment not in {"local", "test", "production"}:
        raise ValueError("ADP_ENV doit être local, test ou production")
    log_level = values.get("ADP_LOG_LEVEL", "INFO").strip().upper()
    if log_level not in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}:
        raise ValueError("ADP_LOG_LEVEL invalide")
    data_dir = values.get("ADP_DATA_DIR", "data").strip()
    if not data_dir:
        raise ValueError("ADP_DATA_DIR ne doit pas être vide")
    return Settings(environment, log_level, Path(data_dir).expanduser())
