{{ config(materialized='table') }}

WITH unique_users AS (
    SELECT
        TRY_CAST(user_id AS INTEGER) AS user_id,
        user_name,
        user_surname,
        _ingested_at,
        ROW_NUMBER() OVER (
            PARTITION BY user_id
            ORDER BY _ingested_at DESC
        ) AS row_num
    FROM {{ source('main', 'bronze_raw_events') }}
    WHERE user_id IS NOT NULL
)

SELECT
    user_id,
    user_name,
    user_surname
FROM unique_users
WHERE row_num = 1
