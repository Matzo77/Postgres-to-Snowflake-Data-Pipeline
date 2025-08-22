{{ config(
    tags=['staging']
) }}

with CTE as (
    select
        {{ get_columns_without_prefix('tpcds', 'ship_mode', 'sm_') }}
    from {{ source('tpcds','ship_mode') }}
)
select * exclude (_AIRBYTE_RAW_ID, _AIRBYTE_EXTRACTED_AT, _AIRBYTE_META, _AIRBYTE_GENERATION_ID) 
from CTE
where ship_mode_sk is not null
    and ship_mode_id is not null