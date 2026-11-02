{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        on_schema_change='fail',
        indexes=[
            {'columns': ['filename_str']},
            {'columns': ['data_ingress_date_dte']},
        ],
    )
}}

-- HISTORICAL_CONDITION_RAW_DATA (accuweather.sql): one row per observation, PAYLOAD keeps the full JSON
-- returned by AccuWeather plus the location fields added at ingestion. Append-only: each run adds the
-- observations of the files that landed since the previous run.
select
    f.object_key::varchar(100)          as filename_str,
    obs.payload                         as payload,
    f.data_ingress_date_dte::timestamp  as data_ingress_date_dte
from {{ source('landing', 'accuweather_raw_files') }} as f
cross join lateral jsonb_array_elements(f.payload) with ordinality as obs (payload, position)
{% if is_incremental() %}
where not exists (
    select 1 from {{ this }} as loaded where loaded.filename_str = f.object_key
)
{% endif %}
order by f.object_key, obs.position
