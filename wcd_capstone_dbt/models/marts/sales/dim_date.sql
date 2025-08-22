{{ config(
    tags=['sales']
) }}

select
    *
from {{ ref('stg_date_dim') }}