-- The view must expose each city and observation time once.
select accuweather_id, local_obs_datetime, count(*) as occurrences
from {{ ref('historical_condition_data_view') }}
group by accuweather_id, local_obs_datetime
having count(*) > 1
