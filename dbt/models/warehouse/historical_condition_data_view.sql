{{ config(materialized='view') }}

-- HISTORICAL_CONDITION_DATA_VIEW (accuweather.sql): same columns and casts as the Snowflake view.
-- Differences: it reads the typed observations table instead of parsing JSON on every query, and each
-- (city, observation time) appears once. Order your queries explicitly (the original ORDER BY of a view
-- is not guaranteed for consumers anyway).
select
    accuweather_id,
    city_name,
    country,
    country_code,
    latitude,
    longitude,
    local_obs_datetime,
    weather_text,
    cloud_cover,
    temp_c,
    real_feel_temp_c,
    temp_f,
    real_feel_temp_f,
    relative_humidity,
    pressure_metric_mb,
    pressure_imperial_inhg,
    uv_index,
    uv_index_text
from {{ ref('historical_condition_observations') }}
