-- Fails if share of rows with all key columns non-null drops below threshold.
SELECT *
FROM (
    SELECT
        100.0
        * SUM(
            CASE
                WHEN
                    event_timestamp IS NOT NULL
                    AND event_type IS NOT NULL
                    AND product_id IS NOT NULL
                    AND user_id IS NOT NULL
                    THEN 1
                ELSE 0
            END
        )
        / NULLIF(COUNT(*), 0) AS completeness_pct
    FROM {{ ref('fact_events') }}
)
WHERE completeness_pct < 95.0
