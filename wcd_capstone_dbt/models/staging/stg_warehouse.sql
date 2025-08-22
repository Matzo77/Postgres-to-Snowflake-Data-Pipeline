{{ config(
    tags=['staging']
) }}

with CTE as (
    select
        {{ get_columns_without_prefix('tpcds', 'warehouse', 'w_') }}
    from {{ source('tpcds','warehouse') }}
)
select * exclude (_AIRBYTE_RAW_ID, _AIRBYTE_EXTRACTED_AT, _AIRBYTE_META, _AIRBYTE_GENERATION_ID) 
from CTE
where warehouse_sk is not null
    and warehouse_id is not null 