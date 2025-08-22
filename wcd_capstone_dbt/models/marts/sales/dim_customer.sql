
{{ config(
    tags=['sales']
) }}

select
    *
from {{ref('int_agg_customer')}}