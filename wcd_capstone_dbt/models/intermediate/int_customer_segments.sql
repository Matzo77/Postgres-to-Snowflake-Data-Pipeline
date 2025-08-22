{{
    config(
        materialized='table',
        tags=['intermediate']
    )
}}
-- getting the most recent date that is available in the sales tables
{% set max_date_query %}
    select max(d.cal_dt) as max_sold_date
    from (
        select max(sold_date_sk) as sold_date_sk
        from {{ ref('stg_web_sales') }}
        union all
        select max(sold_date_sk) as sold_date_sk
        from {{ ref('stg_catalog_sales') }}
    ) temp_table
    join {{ ref('stg_date_dim') }} d
      on temp_table.sold_date_sk = d.date_sk
{% endset %}

{% set results = run_query(max_date_query) %}

{% if execute %}
  {% set max_sold_date = results.columns[0].values()[0] %}
{% else %}
  {% set max_sold_date = "1900-01-01" %}
{% endif %}


-- creating the RFM model
with incremental_sales as (
    select
        bill_customer_sk as customer_sk,
        order_number,
        sold_date_sk,
        (quantity * sales_price) as daily_sales_amt
    from {{ref('stg_catalog_sales')}}
    where daily_sales_amt is not null

    union all

    select
        bill_customer_sk as customer_sk,
        order_number,
        sold_date_sk,
        (quantity * sales_price) as daily_sales_amt
    from {{ref('stg_web_sales')}}
),

customer_rfm as (
    select 
        customer_sk,
        max(sold_date_sk) as recent_purchase_sk,
        count(order_number) as frequency,
        sum(daily_sales_amt) as monetary
    from
        incremental_sales
    group by 1
),

customer_rfm_date as (
    select
        customer_sk,
        d.cal_dt as recent_purchase,
        frequency,
        monetary
    from 
        customer_rfm c
    left join {{ref('stg_date_dim')}} d
        on c.recent_purchase_sk = d.date_sk
),

rfm_create_segments as (
    select
        customer_sk,
        recent_purchase,
        case
            when recent_purchase >= dateadd(day, -30, '{{ max_sold_date }}') then 'Active'
            when recent_purchase < dateadd(day, -30, '{{ max_sold_date }}') and recent_purchase >= dateadd(day, -60, '{{ max_sold_date }}') then 'Lapsing'
            else 'Inactive'
        end as recency_segment,
        case 
            when frequency > 10 then 'High Frequency'
            when frequency between 5 and 10 then 'Medium Frequency'
            else 'Low Frequency'
        end as frequency_segment,
        case
            when monetary > 1000 then 'High Value'
            when monetary between 500 and 1000 then 'Medium Vallue'
            else 'Low Value'
        end as monetary_segment
    from
        customer_rfm_date
),

rfm_final_segment as (
    select
        customer_sk,
        recent_purchase,
        recency_segment,
        frequency_segment,
        monetary_segment,
        case
            when recency_segment = 'Active' and frequency_segment = 'High Frequency' and monetary_segment = 'High Value' then 'Champions'
            when recency_segment = 'Active' and frequency_segment = 'Medium Frequency' and monetary_segment = 'High Value' then 'Champions'
            when recency_segment = 'Lapsing' and frequency_segment = 'High Frequency' and monetary_segment = 'High Value' then 'Potential Loyalists'
            when recency_segment = 'Lapsing' and frequency_segment = 'Medium Frequency' and monetary_segment = 'Medium Value' then 'Hibernating'
            when recency_segment = 'Lapsing' and frequency_segment = 'Medium Frequency' and monetary_segment = 'High Value' then 'Hibernating'
            when recency_segment = 'Active' and frequency_segment = 'Low Frequency' and monetary_segment = 'High Value' then 'New User'
            else 'At Risk'
        end as customer_segment
    from
        rfm_create_segments 
)

select
    customer_sk as segment_customer_sk,
    recent_purchase,
    recency_segment,
    frequency_segment,
    monetary_segment,
    customer_segment
from
    rfm_final_segment