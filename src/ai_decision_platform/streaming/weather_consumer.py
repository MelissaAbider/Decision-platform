"""Kafka consumer qui stocke les événements météo dans PostgreSQL."""

from __future__ import annotations

import argparse
import json
from typing import Any

import psycopg
from kafka import KafkaConsumer

DEFAULT_BOOTSTRAP_SERVERS = "localhost:29092"
DEFAULT_TOPIC = "weather.current"
DEFAULT_GROUP_ID = "weather-current-postgres-writer"
DEFAULT_DATABASE_URL = "postgresql://adp:adp@localhost:5432/adp"

CREATE_TABLE_SQL = """
create schema if not exists streaming;

create table if not exists streaming.weather_current_events (
    event_id text primary key,
    source text not null,
    event_time_utc timestamptz not null,
    fetched_at_utc timestamptz not null,
    consumed_at_utc timestamptz not null default now(),
    latitude double precision not null,
    longitude double precision not null,
    temperature_2m_c double precision,
    relative_humidity_2m double precision,
    wind_speed_10m_kmh double precision,
    raw_payload jsonb not null
);
"""

INSERT_EVENT_SQL = """
insert into streaming.weather_current_events (
    event_id,
    source,
    event_time_utc,
    fetched_at_utc,
    latitude,
    longitude,
    temperature_2m_c,
    relative_humidity_2m,
    wind_speed_10m_kmh,
    raw_payload
)
values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
on conflict (event_id) do update set
    fetched_at_utc = excluded.fetched_at_utc,
    consumed_at_utc = now(),
    temperature_2m_c = excluded.temperature_2m_c,
    relative_humidity_2m = excluded.relative_humidity_2m,
    wind_speed_10m_kmh = excluded.wind_speed_10m_kmh,
    raw_payload = excluded.raw_payload;
"""


def build_consumer(bootstrap_servers: str, topic: str, group_id: str) -> KafkaConsumer:
    return KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        key_deserializer=lambda value: value.decode("utf-8") if value else None,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        consumer_timeout_ms=1000,
    )


def ensure_table(connection: psycopg.Connection[Any]) -> None:
    with connection.cursor() as cursor:
        cursor.execute(CREATE_TABLE_SQL)
    connection.commit()


def insert_event(connection: psycopg.Connection[Any], event: dict[str, Any]) -> None:
    with connection.cursor() as cursor:
        cursor.execute(
            INSERT_EVENT_SQL,
            (
                event["event_id"],
                event["source"],
                event["event_time_utc"],
                event["fetched_at_utc"],
                event["latitude"],
                event["longitude"],
                event.get("temperature_2m_c"),
                event.get("relative_humidity_2m"),
                event.get("wind_speed_10m_kmh"),
                json.dumps(event, sort_keys=True),
            ),
        )
    connection.commit()


def consume_weather_events(
    bootstrap_servers: str,
    topic: str,
    group_id: str,
    database_url: str,
    max_messages: int | None,
) -> None:
    consumer = build_consumer(bootstrap_servers, topic, group_id)
    consumed_count = 0
    try:
        with psycopg.connect(database_url) as connection:
            ensure_table(connection)
            while max_messages is None or consumed_count < max_messages:
                message_batch = consumer.poll(timeout_ms=1000)
                if not message_batch:
                    if max_messages is not None:
                        break
                    continue
                for messages in message_batch.values():
                    for message in messages:
                        insert_event(connection, message.value)
                        consumed_count += 1
                        print(f"Stored {message.value['event_id']} from {topic}")
                        if max_messages is not None and consumed_count >= max_messages:
                            return
    finally:
        consumer.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Stocke les messages météo Kafka dans PostgreSQL")
    parser.add_argument("--bootstrap-servers", default=DEFAULT_BOOTSTRAP_SERVERS)
    parser.add_argument("--topic", default=DEFAULT_TOPIC)
    parser.add_argument("--group-id", default=DEFAULT_GROUP_ID)
    parser.add_argument("--database-url", default=DEFAULT_DATABASE_URL)
    parser.add_argument(
        "--max-messages", type=int, help="Nombre de messages à consommer puis arrêter"
    )
    args = parser.parse_args()

    consume_weather_events(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        group_id=args.group_id,
        database_url=args.database_url,
        max_messages=args.max_messages,
    )


if __name__ == "__main__":
    main()
