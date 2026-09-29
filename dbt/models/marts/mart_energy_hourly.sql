select
    timestamp_utc,
    date_utc,
    consumption_mw,
    temperature_c,
    hour,
    month,
    weekday_iso,
    is_weekend,
    is_public_holiday,
    case
        when hour between 6 and 21 then true
        else false
    end as is_daytime,
    case
        when hour between 7 and 10 then 'morning_peak'
        when hour between 17 and 20 then 'evening_peak'
        when hour between 0 and 5 then 'night'
        else 'standard'
    end as demand_period
from {{ source('serving', 'energy_features_hourly_postgres') }}
