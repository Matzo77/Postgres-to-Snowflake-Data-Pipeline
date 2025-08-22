{{
    config(
        materialized='incremental',
        unique_key=['bill_customer_sk','item_sk','promo_sk','sold_date_sk'],
        tags=['intermediate']
    )
}}

with incremental_sales as (
    select
        bill_customer_sk,
        item_sk,
        promo_sk,
        order_number,
        'Catalog' as channel_key,
        sold_date_sk,
        quantity as daily_qty,
        (quantity * sales_price) as daily_sales_amt,
        net_profit as daily_net_profit,
        ext_wholesale_cost as daily_cogs
    from
        {{ ref('stg_catalog_sales')}}
    where daily_qty is not null
        and daily_sales_amt is not null
        and warehouse_sk is not null

        union all

    select
    bill_customer_sk,
    item_sk,
    promo_sk,
    order_number,
    'Web' as channel_key,
    sold_date_sk,
    quantity as daily_qty,
    (quantity * sales_price) as daily_sales_amt,
    net_profit as daily_net_profit,
    ext_wholesale_cost as daily_cogs
from
    {{ ref('stg_web_sales')}}
where daily_qty is not null
    and daily_sales_amt is not null
    and warehouse_sk is not null
),

update_yr_mnth_week_to_sales as (
    select
        bill_customer_sk,
        item_sk,
        promo_sk,
        channel_key,
        sold_date_sk,
        yr_num as sold_yr_num,
        mnth_num as sold_mnth_num,
        wk_num as sold_wk_num,
        sum(daily_qty) as daily_qty,
        sum(daily_sales_amt) as daily_sales_amt,
        sum(daily_net_profit) as daily_net_profit,
        count(*) as daily_orders,
        sum(daily_cogs) as daily_cogs
    from 
        incremental_sales
    left join
        {{ ref('stg_date_dim')}}
    on sold_date_sk = date_sk
    group by 1,2,3,4,5,6,7,8  
)

select
    bill_customer_sk,
    item_sk,
    promo_sk,
    channel_key,
    sold_date_sk,
    max(sold_yr_num) as sold_yr_num,
    max(sold_mnth_num) as sold_mnth_num,
    max(sold_wk_num) as sold_wk_num,
    sum(daily_qty) as daily_qty,
    sum(daily_sales_amt) as daily_sales_amt,
    sum(daily_net_profit) as daily_net_profit,
    sum(daily_orders) as daily_orders,
    sum(daily_cogs) as daily_cogs
from update_yr_mnth_week_to_sales

{% if is_incremental() %}
    where sold_date_sk >= (select max(sold_date_sk) from {{this}})
{% endif %}

group by 1,2,3,4,5
order by 1,2,3,4,5