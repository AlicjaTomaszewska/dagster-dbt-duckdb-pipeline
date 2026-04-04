{{ config(
    materialized='table',
    tags=['gold', 'mart', 'marketing'],
) }}

-- Grain: (report_date, brand, category_l1). Full refresh from silver for stable aggregates.
WITH events_with_dims AS (
    SELECT
        CAST(f.event_timestamp AS DATE) AS report_date,
        COALESCE(NULLIF(TRIM(CAST(p.brand AS VARCHAR)), ''), '__UNKNOWN__') AS brand,
        COALESCE(
            NULLIF(TRIM(CAST(p.category_l1 AS VARCHAR)), ''),
            '__UNKNOWN__'
        ) AS category_l1,
        f.event_type,
        f.price
    FROM {{ ref('fact_events') }} f
    LEFT JOIN {{ ref('dim_products') }} p ON f.product_id = p.product_id
)

SELECT
    report_date,
    brand,
    category_l1,
    COUNT(*) FILTER (WHERE event_type = 'view') AS total_views,
    COUNT(*) FILTER (WHERE event_type = 'purchase') AS total_purchases,
    COALESCE(
        SUM(CASE WHEN event_type = 'purchase' THEN price END),
        0
    ) AS revenue,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE event_type = 'purchase')
        / NULLIF(COUNT(*) FILTER (WHERE event_type = 'view'), 0),
        2
    ) AS conversion_rate_pct,
    CURRENT_TIMESTAMP AS _refreshed_at
FROM events_with_dims
GROUP BY 1, 2, 3
