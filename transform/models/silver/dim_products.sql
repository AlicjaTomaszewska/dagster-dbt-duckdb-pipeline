{{ config(materialized='table') }}

SELECT DISTINCT
    TRY_CAST(product_id AS INTEGER) AS product_id,
    brand,
    category_code,
    split_part(category_code, '.', 1) AS category_l1,
    split_part(category_code, '.', 2) AS category_l2,
    split_part(category_code, '.', 3) AS category_l3
FROM {{ source('main', 'bronze_raw_events') }}
WHERE product_id IS NOT NULL
