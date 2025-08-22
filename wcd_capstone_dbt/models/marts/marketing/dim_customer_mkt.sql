
{{ config(
    tags=['marketing']
) }}

select
    *
from {{ref('int_agg_customer_mkt')}}