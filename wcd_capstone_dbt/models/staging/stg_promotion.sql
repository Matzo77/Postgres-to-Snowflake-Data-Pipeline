{{ config(
    tags=['staging']
) }}

with CTE as (
    select
        {{ get_columns_without_prefix('tpcds', 'promotion', 'p_') }}
    from {{ source('tpcds','promotion') }}
)
select * exclude (_AIRBYTE_RAW_ID, _AIRBYTE_EXTRACTED_AT, _AIRBYTE_META, _AIRBYTE_GENERATION_ID) 
from CTE
where promo_sk is not null
    and promo_id is not null