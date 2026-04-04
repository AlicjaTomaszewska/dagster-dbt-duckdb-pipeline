{{ config(
    materialized='incremental',
    unique_key=['report_date', 'brand', 'category_l1'],
    incremental_strategy='merge',
    tags=['gold', 'mart', 'marketing'],
) }}

/*
  Incremental strategy:
    1. Identify dates that received new events since the last mart refresh
       (via fact_events._loaded_at vs this._refreshed_at).
    2. Recompute FULL aggregates for those dates from ALL events in fact_events
       (not just the new batch — this guarantees correct cumulative totals).
    3. Merge on (report_date, brand, category_l1) overwrites stale rows
       with the updated aggregates.

  First run (full load): is_incremental() = false → all dates processed.
  Subsequent runs with no new data: affected_dates is empty → zero rows inserted.
*/

WITH affected_dates AS (
    SELECT DISTINCT CAST(event_timestamp AS DATE) AS report_date
    FROM {{ ref('fact_events') }}
    {% if is_incremental() %}
    WHERE _loaded_at > (SELECT max(_refreshed_at) FROM {{ this }})
    {% endif %}
),

events_with_dims AS (
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
    WHERE CAST(f.event_timestamp AS DATE) IN (SELECT report_date FROM affected_dates)
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
