select
    date_utc,
    min(timestamp_utc) as first_timestamp_utc,
    max(timestamp_utc) as last_timestamp_utc,
    count(*) as hourly_observation_count,
    round(avg(consumption_mw)::numeric, 3) as avg_consumption_mw,
    round(min(consumption_mw)::numeric, 3) as min_consumption_mw,
    round(max(consumption_mw)::numeric, 3) as peak_consumption_mw,
    round(avg(temperature_c)::numeric, 3) as avg_temperature_c,
    round(min(temperature_c)::numeric, 3) as min_temperature_c,
    round(max(temperature_c)::numeric, 3) as max_temperature_c,
    bool_or(is_weekend) as is_weekend,
    bool_or(is_public_holiday) as is_public_holiday
from {{ ref('mart_energy_hourly') }}
group by date_utc
