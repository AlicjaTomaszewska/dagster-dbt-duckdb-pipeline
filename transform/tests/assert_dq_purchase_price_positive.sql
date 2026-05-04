-- Validity: purchases must have price > 0 (views may have price 0).
SELECT
    event_id,
    event_type,
    price
FROM {{ ref('fact_events') }}
WHERE event_type = 'purchase'
  AND (price IS NULL OR price <= 0)
