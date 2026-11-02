{{
    config(
        indexes=[
            {'columns': ['accuweather_id', 'local_date'], 'unique': true},
            {'columns': ['local_date']},
        ],
    )
}}

-- One row per city and local calendar day: the grain used for trends, seasonality and comparisons.
select
    accuweather_id,
    city_name,
    country,
    country_code,
    country_code_iso3,
    city_name || ', ' || country                            as city_label,
    latitude,
    longitude,
    local_obs_timestamp::date                               as local_date,
    date_trunc('month', local_obs_timestamp)::date          as local_month,
    count(*)                                                as observations,
    min(temp_c)                                             as temp_min_c,
    max(temp_c)                                             as temp_max_c,
    round(avg(temp_c), 1)                                   as temp_avg_c,
    max(temp_c) - min(temp_c)                               as temp_range_c,
    round(avg(real_feel_temp_c), 1)                         as real_feel_avg_c,
    round(avg(relative_humidity), 1)                        as humidity_avg,
    round(avg(dew_point_c), 1)                              as dew_point_avg_c,
    round(avg(pressure_metric_mb), 1)                       as pressure_avg_mb,
    round(avg(wind_speed_kmh), 1)                           as wind_speed_avg_kmh,
    max(wind_gust_kmh)                                      as wind_gust_max_kmh,
    round(avg(cloud_cover), 1)                              as cloud_cover_avg,
    max(uv_index)                                           as uv_index_max,
    coalesce(sum(precip_1hr_mm), 0)                         as precipitation_mm,
    count(*) filter (where has_precipitation)               as hours_with_precipitation,
    mode() within group (order by weather_text)             as prevailing_weather
from {{ ref('historical_condition_observations') }}
group by
    accuweather_id, city_name, country, country_code, country_code_iso3, latitude, longitude,
    local_obs_timestamp::date, date_trunc('month', local_obs_timestamp)::date
