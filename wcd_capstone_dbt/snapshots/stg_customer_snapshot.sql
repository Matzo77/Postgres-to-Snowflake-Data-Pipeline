{% snapshot stg_customer_snapshot %}
{{
    config(
      target_database='wcd_analytic_eng_capstone',
      target_schema='intermediate',
      unique_key='customer_sk',
      strategy='check',
      check_cols = 'all'
    )
}}

select
    *
from
    {{ ref('stg_customer')}}

{% endsnapshot %}