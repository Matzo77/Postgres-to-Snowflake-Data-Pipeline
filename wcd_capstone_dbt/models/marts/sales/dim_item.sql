{{ config(
    tags=['sales']
) }}

select
    *
from {{ ref('stg_item') }}