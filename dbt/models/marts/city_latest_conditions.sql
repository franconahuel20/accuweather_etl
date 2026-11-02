-- Latest observation of every city plus its last-24-hour summary: feeds the world maps and the
-- "current conditions" table of the dashboard.
with observations as (
    select * from {{ ref('historical_condition_observations') }}
),

latest as (
    select distinct on (accuweather_id) *
    from observations
    order by accuweather_id, local_obs_datetime desc
),

last_24h as (
    select
        o.accuweather_id,
        min(o.temp_c)                   as temp_min_24h_c,
        max(o.temp_c)                   as temp_max_24h_c,
        round(avg(o.temp_c), 1)         as temp_avg_24h_c,
        round(avg(o.relative_humidity), 0) as humidity_avg_24h,
        coalesce(sum(o.precip_1hr_mm), 0) as precipitation_24h_mm,
        count(*)                        as observations_24h
    from observations as o
    inner join latest as l
        on l.accuweather_id = o.accuweather_id
       and o.local_obs_datetime > l.local_obs_datetime - interval '24 hours'
    group by o.accuweather_id
)

select
    l.accuweather_id,
    l.city_name,
    l.country,
    l.country_code,
    l.country_code_iso3,
    l.city_name || ', ' || l.country                    as city_label,
    l.latitude,
    l.longitude,
    l.time_zone,
    l.local_obs_datetime                                as observed_at,
    l.local_obs_timestamp                               as observed_at_local,
    l.weather_text,
    l.is_daytime,
    l.temp_c,
    l.real_feel_temp_c,
    l.temp_f,
    l.relative_humidity,
    l.dew_point_c,
    l.wind_speed_kmh,
    l.wind_direction,
    l.cloud_cover,
    l.uv_index,
    l.uv_index_text,
    l.pressure_metric_mb,
    l.visibility_km,
    case
        when l.temp_c < 0 then '1. Freezing (< 0 °C)'
        when l.temp_c < 10 then '2. Cold (0-10 °C)'
        when l.temp_c < 18 then '3. Cool (10-18 °C)'
        when l.temp_c < 25 then '4. Mild (18-25 °C)'
        when l.temp_c < 32 then '5. Warm (25-32 °C)'
        else '6. Hot (>= 32 °C)'
    end                                                 as temperature_band,
    h.temp_min_24h_c,
    h.temp_max_24h_c,
    h.temp_avg_24h_c,
    h.humidity_avg_24h,
    h.precipitation_24h_mm,
    h.observations_24h
from latest as l
left join last_24h as h
    on h.accuweather_id = l.accuweather_id
