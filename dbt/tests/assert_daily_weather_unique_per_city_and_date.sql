select accuweather_id, local_date, count(*) as occurrences
from {{ ref('city_daily_weather') }}
group by accuweather_id, local_date
having count(*) > 1
