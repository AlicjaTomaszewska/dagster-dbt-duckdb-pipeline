{{ config(
    materialized='incremental',
    unique_key='product_id',
    incremental_strategy='merge',
) }}

-- Latest product attributes per product_id from recently ingested bronze rows.
WITH ranked AS (
    SELECT
        TRY_CAST(product_id AS INTEGER) AS product_id,
        brand,
        category_code,
        split_part(category_code, '.', 1) AS category_l1,
        split_part(category_code, '.', 2) AS category_l2,
        split_part(category_code, '.', 3) AS category_l3,
        _ingested_at,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY _ingested_at DESC) AS rn
    FROM {{ source('main', 'bronze_raw_events') }}
    WHERE product_id IS NOT NULL
    {% if is_incremental() %}
      AND _ingested_at > (SELECT max(_updated_at) FROM {{ this }})
    {% endif %}
)

SELECT
    product_id,
    brand,
    category_code,
    category_l1,
    category_l2,
    category_l3,
    _ingested_at AS _updated_at
FROM ranked
WHERE rn = 1
