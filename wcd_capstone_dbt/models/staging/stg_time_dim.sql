{{ config(
    tags=['staging']
) }}

with CTE as (
    select
        {{ get_columns_without_prefix('tpcds', 'time_dim', 't_') }}
    from {{ source('tpcds','time_dim') }}
)
select * exclude (_AIRBYTE_RAW_ID, _AIRBYTE_EXTRACTED_AT, _AIRBYTE_META, _AIRBYTE_GENERATION_ID) 
from CTE
where time_sk is not null
    and time_id is not null
    and time is not null