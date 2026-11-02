{{ config(materialized='view') }}

-- Hourly observations with the local calendar parts used by the hour-of-day and seasonal charts.
select
    accuweather_id,
    city_name,
    country,
    country_code,
    country_code_iso3,
    city_name || ', ' || country                    as city_label,
    latitude,
    longitude,
    local_obs_datetime,
    local_obs_timestamp,
    local_obs_timestamp::date                       as local_date,
    extract(hour from local_obs_timestamp)::int     as local_hour,
    to_char(local_obs_timestamp, 'YYYY-MM')         as local_year_month,
    weather_text,
    -- AccuWeather returns ~35 phrasings; analysis needs a handful of comparable groups.
    case
        when weather_text ilike '%thunder%' then 'Thunderstorms'
        when weather_text ilike '%rain%' or weather_text ilike '%shower%' or weather_text ilike '%drizzle%' then 'Rain & drizzle'
        when weather_text ilike '%snow%' or weather_text ilike '%ice%' or weather_text ilike '%sleet%'
          or weather_text ilike '%flurr%' then 'Snow & ice'
        when weather_text ilike '%fog%' or weather_text ilike '%mist%' or weather_text ilike '%haz%' then 'Fog, mist & haze'
        when weather_text in ('Cloudy', 'Mostly cloudy', 'Overcast') then 'Cloudy'
        when weather_text in ('Partly sunny', 'Clouds and sun', 'Some clouds', 'Partly cloudy', 'Intermittent clouds')
            then 'Partly cloudy'
        when weather_text in ('Clear', 'Sunny', 'Mostly sunny', 'Mostly clear') then 'Clear & sunny'
        else 'Other'
    end                                             as weather_group,
    is_daytime,
    has_precipitation,
    cloud_cover,
    temp_c,
    real_feel_temp_c,
    relative_humidity,
    dew_point_c,
    pressure_metric_mb,
    wind_speed_kmh,
    precip_1hr_mm,
    uv_index,
    uv_index_text
from {{ ref('historical_condition_observations') }}
