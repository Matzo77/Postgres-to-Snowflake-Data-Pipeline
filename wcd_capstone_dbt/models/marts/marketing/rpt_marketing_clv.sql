{{
    config(
        materialized='table',
        tags=['marketing']
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

-- clv analysis

with customer_orders as (
    select
        bill_customer_sk,
        sold_date_sk,
        order_number,
        (quantity * sales_price) as sales_amt,
        net_profit as daily_net_profit,
        ext_wholesale_cost as daily_cogs
    from
        {{ ref('stg_catalog_sales')}}
    where sales_amt is not null
        and warehouse_sk is not null

    union all
    
    select
        bill_customer_sk,
        sold_date_sk,
        order_number,
        (quantity * sales_price) as sales_amt,
        net_profit as daily_net_profit,
        ext_wholesale_cost as daily_cogs
    from
        {{ ref('stg_web_sales')}}
    where sales_amt is not null
        and warehouse_sk is not null
),

convert_to_date as (
    select
        co.bill_customer_sk,
        d.cal_dt as sold_date,
        co.order_number,
        co.sales_amt,
        co.daily_net_profit,
        co.daily_cogs
    from
        customer_orders co
    inner join
        {{ ref('stg_date_dim')}} d
    on co.sold_date_sk = d.date_sk
),

customer_cohorts as (
    select
        bill_customer_sk,
        min(sold_date) as first_purchase_date,
        max(sold_date) as last_purchase_date
    from
        convert_to_date
    group by 1
),

churn_analysis as (
    select 
        year(first_purchase_date) as cohort_year,
        count(bill_customer_sk) as total_customers,
        count(case when last_purchase_date < dateadd(day,-110,'{{ max_sold_date }}') then bill_customer_sk end) as churned_customers
    from
        customer_cohorts
    group by 1
),

estimated_churn as (
    select
        cohort_year,
        1.0 / (churned_customers / total_customers) as estimated_lifespan
    from churn_analysis
),

avg_estimate_churn as (
    select
        1 as lifespan_id,
        avg(estimated_lifespan) as estimated_lifespan
    from estimated_churn
    where cohort_year != year(to_date('{{ max_sold_date }}', 'YYYY-MM-DD'))
),

customer_summary as (
    select
        bill_customer_sk as customer_sk,
        sum(sales_amt) as sum_sales,
        sum(daily_net_profit) as sum_net_profit,
        sum(daily_net_profit)/count(distinct order_number) as avg_order_value,
        count(distinct order_number) as number_of_orders
    from convert_to_date
    group by 1
),

customer_stats_with_clv as (
    select
        cs.customer_sk,
        cs.sum_sales,
        cs.sum_net_profit,
        cs.number_of_orders,
        cs.avg_order_value,
        l.estimated_lifespan,
        (cs.avg_order_value * cs.number_of_orders * l.estimated_lifespan) as CLV
    from customer_summary cs
    cross join avg_estimate_churn l
)

select 
    *
from 
    customer_stats_with_clv
order by CLV desc