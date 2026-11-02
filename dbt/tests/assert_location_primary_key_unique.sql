-- PK_CITIES of accuweather.sql: (COUNTRY_ISO_ALPHA_3_STR, CITY_ACCUWEATHER_ID_STR).
select country_iso_alpha_3_str, city_accuweather_id_str, count(*) as occurrences
from {{ ref('location_data') }}
group by country_iso_alpha_3_str, city_accuweather_id_str
having count(*) > 1
