select
    date_utc,
    demand_period,
    count(*) as hourly_observation_count,
    round(avg(consumption_mw)::numeric, 3) as avg_consumption_mw,
    round(max(consumption_mw)::numeric, 3) as peak_consumption_mw,
    round(avg(temperature_c)::numeric, 3) as avg_temperature_c,
    bool_or(is_weekend) as is_weekend,
    bool_or(is_public_holiday) as is_public_holiday
from {{ ref('mart_energy_hourly') }}
group by date_utc, demand_period
