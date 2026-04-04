{{ config(
    materialized='incremental',
    unique_key='user_id',
    incremental_strategy='merge',
) }}

-- Latest user name/surname per user_id from recently ingested bronze rows.
WITH ranked AS (
    SELECT
        TRY_CAST(user_id AS INTEGER) AS user_id,
        user_name,
        user_surname,
        _ingested_at,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY _ingested_at DESC) AS rn
    FROM {{ source('main', 'bronze_raw_events') }}
    WHERE user_id IS NOT NULL
    {% if is_incremental() %}
      AND _ingested_at > (SELECT max(_updated_at) FROM {{ this }})
    {% endif %}
)

SELECT
    user_id,
    user_name,
    user_surname,
    _ingested_at AS _updated_at
FROM ranked
WHERE rn = 1
