{{ config(severity='warn') }}
-- Warns when an active city never delivered data (wrong AccuWeather key, new city not fetched yet...).
select loc.city_accuweather_id_str, loc.city_name_str
from {{ ref('location_data') }} as loc
left join {{ ref('city_latest_conditions') }} as latest
    on latest.accuweather_id = loc.city_accuweather_id_str
where loc.active_bit and latest.accuweather_id is null
