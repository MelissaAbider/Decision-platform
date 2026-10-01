"""Kafka producer qui publie la météo live Open-Meteo."""

from __future__ import annotations

import argparse
import json
import time
from datetime import UTC, datetime
from typing import Any
from urllib.parse import urlencode
from urllib.request import urlopen

from kafka import KafkaProducer

DEFAULT_TOPIC = "weather.current"
DEFAULT_BOOTSTRAP_SERVERS = "localhost:29092"
OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"


def utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def fetch_current_weather(latitude: float, longitude: float) -> dict[str, Any]:
    query = urlencode(
        {
            "latitude": latitude,
            "longitude": longitude,
            "current": "temperature_2m,relative_humidity_2m,wind_speed_10m",
            "timezone": "UTC",
        }
    )
    url = f"{OPEN_METEO_URL}?{query}"
    with urlopen(url, timeout=20) as response:  # noqa: S310 - URL is a fixed public API.
        payload = json.loads(response.read().decode("utf-8"))

    current = payload["current"]
    current_units = payload.get("current_units", {})
    fetched_at_utc = utc_now_iso()
    event_time_utc = current["time"].replace("+00:00", "Z")

    return {
        "event_id": f"open-meteo:{latitude}:{longitude}:{event_time_utc}",
        "source": "open-meteo",
        "event_time_utc": event_time_utc,
        "fetched_at_utc": fetched_at_utc,
        "latitude": float(payload.get("latitude", latitude)),
        "longitude": float(payload.get("longitude", longitude)),
        "temperature_2m_c": current.get("temperature_2m"),
        "relative_humidity_2m": current.get("relative_humidity_2m"),
        "wind_speed_10m_kmh": current.get("wind_speed_10m"),
        "units": {
            "temperature_2m": current_units.get("temperature_2m", "°C"),
            "relative_humidity_2m": current_units.get("relative_humidity_2m", "%"),
            "wind_speed_10m": current_units.get("wind_speed_10m", "km/h"),
        },
    }


def build_producer(bootstrap_servers: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        key_serializer=lambda value: value.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value, sort_keys=True).encode("utf-8"),
    )


def publish_weather_events(
    bootstrap_servers: str,
    topic: str,
    latitude: float,
    longitude: float,
    interval_seconds: int,
    max_events: int | None,
) -> None:
    producer = build_producer(bootstrap_servers)
    sent_count = 0
    try:
        while max_events is None or sent_count < max_events:
            event = fetch_current_weather(latitude, longitude)
            producer.send(topic, key=event["event_id"], value=event)
            producer.flush()
            sent_count += 1
            print(f"Published {event['event_id']} to {topic}")
            if max_events is None or sent_count < max_events:
                time.sleep(interval_seconds)
    finally:
        producer.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Publie la météo live Open-Meteo dans Kafka")
    parser.add_argument("--bootstrap-servers", default=DEFAULT_BOOTSTRAP_SERVERS)
    parser.add_argument("--topic", default=DEFAULT_TOPIC)
    parser.add_argument("--latitude", type=float, default=48.8566, help="Latitude de Paris")
    parser.add_argument("--longitude", type=float, default=2.3522, help="Longitude de Paris")
    parser.add_argument("--interval-seconds", type=int, default=60)
    parser.add_argument("--max-events", type=int, help="Nombre de messages à publier puis arrêter")
    args = parser.parse_args()

    publish_weather_events(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        latitude=args.latitude,
        longitude=args.longitude,
        interval_seconds=args.interval_seconds,
        max_events=args.max_events,
    )


if __name__ == "__main__":
    main()
