{{ config(
    tags=['staging']
) }}

with CTE as (
    select
        {{ get_columns_without_prefix('tpcds', 'customer_address', 'ca_') }}
    from {{ source('tpcds','customer_address') }}
)
select * exclude (_AIRBYTE_RAW_ID, _AIRBYTE_EXTRACTED_AT, _AIRBYTE_META, _AIRBYTE_GENERATION_ID) 
from CTE
where address_sk is not null
    and address_id is not null
    and city is not null
    and county is not null
    and state is not null
    and zip is not null
    and country is not null