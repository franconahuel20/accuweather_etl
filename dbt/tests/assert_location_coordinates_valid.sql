select city_name_str, latitude_flt, longitude_flt
from {{ ref('location_data') }}
where latitude_flt not between -90 and 90
   or longitude_flt not between -180 and 180
