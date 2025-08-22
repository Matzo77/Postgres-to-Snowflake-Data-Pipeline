{{ config(
    tags=['marketing']
) }}

select 
    *
from
    {{ ref('int_agg_daily_sales_mkt')}}