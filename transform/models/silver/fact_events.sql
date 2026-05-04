{{ config(
    materialized='incremental',
    incremental_strategy='append',
) }}

{#-
  Dedupe within each incremental batch: NOT EXISTS only compares to {{ this }}, so duplicate
  event_ids in the same INSERT would all slip through. One row per event_id per batch (latest bronze _ingested_at wins).
-#}

WITH bronzed AS (
    SELECT
        md5(
            cast(event_time as varchar) || cast(event_type as varchar)
            || cast(user_id as varchar) || cast(product_id as varchar)
            || cast(user_session as varchar)
        ) AS event_id,
        TRY_CAST(event_time AS TIMESTAMP) AS event_timestamp,
        event_type,
        TRY_CAST(product_id AS INTEGER) AS product_id,
        TRY_CAST(user_id AS INTEGER) AS user_id,
        TRY_CAST(price AS DOUBLE) AS price,
        user_session,
        _file_name AS source_file,
        _ingested_at
    FROM {{ source('main', 'bronze_raw_events') }}

    {% if is_incremental() %}
      WHERE _ingested_at > (
          SELECT COALESCE(MAX(_loaded_at), TIMESTAMP '1970-01-01')
          FROM {{ this }}
      )
    {% endif %}
),

staged AS (
    SELECT
        event_id,
        event_timestamp,
        event_type,
        product_id,
        user_id,
        price,
        user_session,
        source_file,
        CURRENT_TIMESTAMP AS _loaded_at
    FROM bronzed
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY event_id
        ORDER BY _ingested_at DESC, source_file DESC
    ) = 1
)

SELECT *
FROM staged
{% if is_incremental() %}
WHERE NOT EXISTS (
    SELECT 1
    FROM {{ this }} existing
    WHERE existing.event_id = staged.event_id
)
{% endif %}
