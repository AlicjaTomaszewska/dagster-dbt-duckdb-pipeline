{{ config(
    materialized='incremental',
    unique_key='event_id',
    incremental_strategy='merge',
) }}

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
    CURRENT_TIMESTAMP AS _loaded_at
FROM {{ source('main', 'bronze_raw_events') }}

{% if is_incremental() %}
  WHERE _ingested_at > (SELECT max(_loaded_at) FROM {{ this }})
{% endif %}
