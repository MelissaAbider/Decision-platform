SELECT
    COUNT(*) AS rows_count,
    MIN(timestamp_utc) AS min_timestamp_utc,
    MAX(timestamp_utc) AS max_timestamp_utc,
    MIN(date_utc) AS min_date_utc,
    MAX(date_utc) AS max_date_utc
FROM public.energy_features_hourly_postgres;

SELECT
    date_utc,
    ROUND(AVG(consumption_mw)::numeric, 2) AS avg_consumption_mw,
    ROUND(MAX(consumption_mw)::numeric, 2) AS max_consumption_mw,
    ROUND(AVG(temperature_c)::numeric, 2) AS avg_temperature_c
FROM public.energy_features_hourly_postgres
GROUP BY date_utc
ORDER BY date_utc;
