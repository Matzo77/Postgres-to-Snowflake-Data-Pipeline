{{ config(
    tags=['marketing']
) }}

select 
    bill_customer_sk,
    sold_yr_num,
    sold_mnth_num,
    sold_wk_num,
    sum(daily_qty) as sum_qty_wk,
    sum(daily_sales_amt) as sum_amt_wk,
    sum(daily_net_profit) as sum_profit_wk,
    sum(daily_orders) as sum_orders_wk,
    sum(daily_cogs) as sum_cogs_wk
from 
    {{ref('int_agg_daily_sales_mkt')}}
group by 1,2,3,4
order by 2,3,4,5