{{ config(
    tags=['sales']
) }}

select 
    agg_d.yr_wk_num,
    agg_d.warehouse_sk,
    agg_d.item_sk,
    sum(daily_qty) as sum_qty_wk,
    sum(daily_sales_amt) as sum_amt_wk,
    sum(daily_net_profit) as sum_profit_wk,
    sum_qty_wk/7 as avg_qty_dy, 
    coalesce(sum(inv.quantity_on_hand), 0) as inv_on_hand_qty_wk,
    coalesce(inv_on_hand_qty_wk, 0) / sum_qty_wk as wks_sply,
    case when (avg_qty_dy > 0 and avg_qty_dy > inv_on_hand_qty_wk) then true else false end as low_stock_flg_wk,
    min(min(agg_d.sold_date_sk)) over (partition by agg_d.yr_wk_num) as week_start_sk,
    max(max(agg_d.sold_date_sk)) over (partition by agg_d.yr_wk_num) as week_end_sk
from {{ref('int_agg_daily_sales')}} agg_d
inner join {{ref('int_date_inventory')}} inv
using(yr_wk_num,warehouse_sk,item_sk)
group by 1,2,3
order by 1,2,3