

{{ config(
    tags=['intermediate']
) }}

select
    {{ dbt_utils.generate_surrogate_key([
        'customer_sk',
        'dbt_valid_from'
    ]) }} as customer_key,
    customer_sk,
    salutation,
    first_name,
    last_name,
    preferred_cust_flag,
    login,
    email_address,
    birth_day,
    birth_month,
    birth_year,
    birth_country,
    d_sale.cal_dt as first_sales_date,
    d_ship.cal_dt as first_shipto_date,
    d_review.cal_dt as last_review_date,
    street_number,
    street_name,
    street_type,
    suite_number,
    city,
    county,
    state,
    zip,
    country,
    gmt_offset,
    location_type,
    gender,
    marital_status,
    education_status,
    purchase_estimate,
    credit_rating,
    cd.dep_count as customer_dep_count,
    dep_employed_count,
    dep_college_count,
    buy_potential,
    hd.dep_count as household_dep_count,
    vehicle_count,
    ib.lower_bound as income_lower_bound,
    ib.upper_bound as income_upper_bound,
    recency_segment,
    frequency_segment,
    monetary_segment,
    customer_segment,
    c.dbt_valid_from as start_date,
    c.dbt_valid_to as end_date,
    case when c.dbt_valid_to is null then true else false end as is_active
from {{ ref('stg_customer_snapshot') }} c
left join {{ ref('stg_date_dim')}} as d_sale on c.first_sales_date_sk = d_sale.date_sk
left join {{ ref('stg_date_dim')}} as d_ship on first_shipto_date_sk = d_ship.date_sk
left join {{ ref('stg_date_dim')}} as d_review on last_review_date_sk = d_review.date_sk
left join {{ ref('stg_customer_address') }} ca on c.current_addr_sk = ca.address_sk
left join {{ ref('stg_customer_demographics') }} cd on c.current_cdemo_sk = cd.demo_sk
left join {{ ref('stg_household_demographics') }} hd on c.current_hdemo_sk = hd.demo_sk
left join {{ ref('stg_income_band') }} ib on hd.income_band_sk = ib.income_band_sk
left join {{ ref('int_customer_segments')}} seg on c.customer_sk = seg.segment_customer_sk
where end_date is null
    and recency_segment is not null
    and frequency_segment is not null
    and monetary_segment is not null
    and customer_segment is not null