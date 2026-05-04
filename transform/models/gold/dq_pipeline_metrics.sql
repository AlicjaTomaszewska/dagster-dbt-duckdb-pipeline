{{ config(
    materialized='table',
    tags=['gold', 'data_quality'],
) }}

/*
  One row per metric (latest snapshot each full dbt build of this model).
  Downstream consumers and the data contract reference these names.

  Requires silver fact_events to exist first, e.g.:
    dbt build --select +dq_pipeline_metrics
  (not plain --select dq_pipeline_metrics alone).
*/

WITH fact AS (
    SELECT * FROM {{ ref('fact_events') }}
),
agg AS (
    SELECT
        COUNT(*) AS row_cnt,
        COUNT(DISTINCT event_id) AS distinct_event_id_cnt,
        SUM(
            CASE
                WHEN
                    event_timestamp IS NOT NULL
                    AND event_type IS NOT NULL
                    AND product_id IS NOT NULL
                    AND user_id IS NOT NULL
                    THEN 1
                ELSE 0
            END
        ) AS complete_key_rows,
        SUM(
            CASE
                WHEN event_type = 'purchase' AND (price IS NULL OR price <= 0)
                    THEN 1
                ELSE 0
            END
        ) AS invalid_purchase_price_rows,
        MAX(_loaded_at) AS max_loaded_at
    FROM fact
)
SELECT
    'silver_fact_events_row_count' AS metric_name,
    CAST(a.row_cnt AS VARCHAR) AS metric_value,
    'count' AS unit,
    CURRENT_TIMESTAMP AS measured_at
FROM agg a

UNION ALL
SELECT
    'silver_fact_events_completeness_key_cols_pct',
    CAST(
        ROUND(100.0 * a.complete_key_rows / NULLIF(a.row_cnt, 0), 4) AS VARCHAR
    ),
    'percent',
    CURRENT_TIMESTAMP
FROM agg a

UNION ALL
SELECT
    'silver_fact_events_event_id_uniqueness_pct',
    CAST(
        ROUND(100.0 * a.distinct_event_id_cnt / NULLIF(a.row_cnt, 0), 4) AS VARCHAR
    ),
    'percent',
    CURRENT_TIMESTAMP
FROM agg a

UNION ALL
SELECT
    'silver_fact_events_freshness_hours',
    CAST(
        ROUND(
            date_diff('second', a.max_loaded_at, CURRENT_TIMESTAMP) / 3600.0,
            4
        ) AS VARCHAR
    ),
    'hours',
    CURRENT_TIMESTAMP
FROM agg a
WHERE a.row_cnt > 0

UNION ALL
SELECT
    'silver_fact_events_invalid_purchase_price_row_count',
    CAST(a.invalid_purchase_price_rows AS VARCHAR),
    'count',
    CURRENT_TIMESTAMP
FROM agg a
