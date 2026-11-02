{{ config(materialized='table') }}

-- HISTORICAL_CONDITION_RAW_DATA_CLONE (accuweather.sql): point-in-time backup copy of the raw table,
-- loaded from its Snowflake export. Daily runs do not change it.
select
    filename_str::varchar(100)          as filename_str,
    payload                             as payload,
    data_ingress_date_dte::timestamp    as data_ingress_date_dte
from {{ source('landing', 'historical_condition_raw_data_clone_export') }}
