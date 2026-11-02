{{
    config(
        materialized='table',
        indexes=[
            {'columns': ['accuweather_id', 'local_obs_datetime'], 'unique': true},
            {'columns': ['local_obs_datetime']},
        ],
    )
}}

-- Typed, de-duplicated observations parsed from the raw JSON once, so the view and the marts never
-- re-parse 1+ GB of JSON. AccuWeather's "historical/24" windows can overlap (reruns, manual fetches),
-- so each (city, observation time) is kept once: the first file that delivered it wins.
with parsed as (
    select
        payload ->> 'city_accuweather_id'                                   as accuweather_id,
        payload ->> 'city_name'                                             as city_name,
        payload ->> 'country_name'                                          as country,
        payload ->> 'country_iso_code'                                      as country_code,
        (payload ->> 'latitude')::double precision                          as latitude,
        (payload ->> 'longitude')::double precision                         as longitude,
        (payload ->> 'LocalObservationDateTime')::timestamptz               as local_obs_datetime,
        -- wall-clock time of the city (the offset is dropped on purpose for hour/day analysis)
        left(payload ->> 'LocalObservationDateTime', 19)::timestamp         as local_obs_timestamp,
        (payload ->> 'EpochTime')::bigint                                   as epoch_time,
        payload ->> 'WeatherText'                                           as weather_text,
        (payload ->> 'WeatherIcon')::smallint                               as weather_icon,
        (payload ->> 'IsDayTime')::boolean                                  as is_daytime,
        (payload ->> 'HasPrecipitation')::boolean                           as has_precipitation,
        payload ->> 'PrecipitationType'                                     as precipitation_type,
        (payload ->> 'CloudCover')::numeric(3, 0)                           as cloud_cover,
        (payload #>> '{Temperature,Metric,Value}')::numeric(3, 1)           as temp_c,
        (payload #>> '{RealFeelTemperature,Metric,Value}')::numeric(3, 1)   as real_feel_temp_c,
        (payload #>> '{Temperature,Imperial,Value}')::numeric(3, 0)         as temp_f,
        (payload #>> '{RealFeelTemperature,Imperial,Value}')::numeric(3, 0) as real_feel_temp_f,
        (payload ->> 'RelativeHumidity')::numeric                           as relative_humidity,
        (payload #>> '{DewPoint,Metric,Value}')::numeric                    as dew_point_c,
        (payload #>> '{Pressure,Metric,Value}')::numeric                    as pressure_metric_mb,
        (payload #>> '{Pressure,Imperial,Value}')::numeric                  as pressure_imperial_inhg,
        payload #>> '{PressureTendency,LocalizedText}'                      as pressure_tendency,
        (payload #>> '{Wind,Speed,Metric,Value}')::numeric                  as wind_speed_kmh,
        payload #>> '{Wind,Direction,English}'                              as wind_direction,
        (payload #>> '{WindGust,Speed,Metric,Value}')::numeric              as wind_gust_kmh,
        (payload #>> '{Visibility,Metric,Value}')::numeric                  as visibility_km,
        (payload #>> '{Precip1hr,Metric,Value}')::numeric                   as precip_1hr_mm,
        (payload ->> 'UVIndex')::numeric(3, 0)                              as uv_index,
        payload ->> 'UVIndexText'                                           as uv_index_text,
        filename_str,
        data_ingress_date_dte
    from {{ ref('historical_condition_raw_data') }}
),

deduplicated as (
    select distinct on (accuweather_id, epoch_time) *
    from parsed
    order by accuweather_id, epoch_time, data_ingress_date_dte, filename_str
)

select
    d.*,
    loc.country_iso_alpha_3_str                                             as country_code_iso3,
    loc.tz_str                                                              as time_zone
from deduplicated as d
left join {{ ref('location_data') }} as loc
    on loc.city_accuweather_id_str = d.accuweather_id
