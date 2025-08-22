{{ config(
    materialized='incremental',
    unique_key=['date_sk','warehouse_sk', 'item_sk'],
    tags=['intermediate']
) }}

select d.yr_wk_num, i.*
from {{ ref('stg_inventory') }} as i
left join {{ ref('stg_date_dim') }} as d
using(date_sk)

{% if is_incremental() %}
    where date_sk >= (select max(date_sk) from {{this}})
{% endif %}

order by 1