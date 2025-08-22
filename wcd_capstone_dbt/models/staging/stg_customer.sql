{{ config(
    tags=['staging']
) }}

with CTE as (
    select
        {{ get_columns_without_prefix('tpcds', 'customer', 'c_') }}
    from {{ source('tpcds','customer') }}
)
select * exclude (_AIRBYTE_RAW_ID, _AIRBYTE_EXTRACTED_AT, _AIRBYTE_META, _AIRBYTE_GENERATION_ID) 
from CTE
where customer_sk is not null
    and customer_id is not null
    and first_name is not null
    and last_name is not null
    and email_address is not null