{{ config(severity='warn') }}
-- Flags sensor/API glitches: values outside the range ever recorded on Earth.
select accuweather_id, local_obs_datetime, temp_c, relative_humidity
from {{ ref('historical_condition_observations') }}
where temp_c not between -90 and 60
   or relative_humidity not between 0 and 100
