
{{ config(
    materialized='incremental',
    unique_key=['warehouse_sk', 'item_sk', 'sold_date_sk'],
    tags=['intermediate']
) }}

with combined_daily_sales as (
    select 
        warehouse_sk,
        item_sk,
        sold_date_sk,
        quantity as daily_qty,
        (quantity * sales_price) as daily_sales_amt,
        net_profit as daily_net_profit
from {{ ref('stg_web_sales') }}
where daily_qty is not null and daily_qty > 0
    and daily_sales_amt is not null and daily_sales_amt > 0
    and warehouse_sk is not null
union all
    select 
        warehouse_sk,
        item_sk,
        sold_date_sk,
        quantity as daily_qty,
        (quantity * sales_price) as daily_sales_amt,
        net_profit as daily_net_profit
from {{ ref('stg_catalog_sales')}}
where daily_qty is not null and daily_qty > 0
    and daily_sales_amt is not null and daily_sales_amt > 0
    and warehouse_sk is not null
),
agg_daily_sales as (
    select 
        warehouse_sk,
        item_sk,
        sold_date_sk,
        sum(daily_qty) as daily_qty,
        sum(daily_sales_amt) as daily_sales_amt,
        sum(daily_net_profit) as daily_net_profit
    from combined_daily_sales
    group by 1, 2, 3
)
select d.yr_wk_num, agg_d.* from agg_daily_sales agg_d
left join {{ ref('stg_date_dim') }} d on agg_d.sold_date_sk = d.date_sk

{% if is_incremental() %}

    where sold_date_sk >= (select max(sold_date_sk) from {{this}})

{% endif %}

order by 1,4,2,3