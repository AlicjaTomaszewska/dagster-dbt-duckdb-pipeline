-- Fails if duplicate event_id exists in silver fact_events.
SELECT
    event_id,
    COUNT(*) AS cnt
FROM {{ ref('fact_events') }}
GROUP BY 1
HAVING COUNT(*) > 1
