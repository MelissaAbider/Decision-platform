select
    count(*) as event_count,
    min(event_time_utc) as first_event_time_utc,
    max(event_time_utc) as last_event_time_utc,
    max(consumed_at_utc) as last_consumed_at_utc
from streaming.weather_current_events;

select
    event_time_utc,
    fetched_at_utc,
    temperature_2m_c,
    relative_humidity_2m,
    wind_speed_10m_kmh,
    latitude,
    longitude
from streaming.weather_current_events
order by consumed_at_utc desc
limit 10;
