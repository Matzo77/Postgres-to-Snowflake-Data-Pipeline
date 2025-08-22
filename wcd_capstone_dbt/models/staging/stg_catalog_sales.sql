{{ config(
    tags=['staging']
) }}

with CTE as (
    select
        {{ get_columns_without_prefix('tpcds', 'catalog_sales', 'cs_') }}
    from {{ source('tpcds','catalog_sales') }}
)
select * exclude (_AIRBYTE_RAW_ID, _AIRBYTE_EXTRACTED_AT, _AIRBYTE_META, _AIRBYTE_GENERATION_ID) 
from CTE
where sold_date_sk is not null
    and bill_customer_sk is not null
    and warehouse_sk is not null
    and item_sk is not null
    and order_number is not null
    and quantity is not null
    and sales_price is not null