{{ config(
    tags=['sales']
) }}

SELECT
    *
FROM {{ ref('stg_warehouse') }}