
{{ config(
    tags=['sales']
) }}

select * 
from {{ref('fct_sales_inv_weekly')}}
where yr_wk_num = (
    select max(yr_wk_num)
    from {{ref('fct_sales_inv_weekly')}}
)