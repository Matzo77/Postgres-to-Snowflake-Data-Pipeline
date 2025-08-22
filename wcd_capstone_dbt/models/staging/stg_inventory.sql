{{ config(
    tags=['staging']
) }}

select
    {{ get_columns_without_prefix('tpcds', 'inventory', 'inv_') }}
from {{ source('tpcds','inventory') }}
where item_sk is not null
    and warehouse_sk is not null