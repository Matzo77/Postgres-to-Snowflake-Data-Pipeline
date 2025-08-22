{{ config(
    tags=['staging']
) }}

with CTE as (
    select
        {{ get_columns_without_prefix('tpcds', 'customer_demographics', 'cd_') }}
    from {{ source('tpcds','customer_demographics') }}
)
select * exclude (_AIRBYTE_RAW_ID, _AIRBYTE_EXTRACTED_AT, _AIRBYTE_META, _AIRBYTE_GENERATION_ID) 
from CTE
where demo_sk is not null