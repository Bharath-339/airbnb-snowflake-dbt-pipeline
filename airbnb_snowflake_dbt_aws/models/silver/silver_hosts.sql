{{ config(
    materialized='incremental',
    unique_key='HOST_ID'
) }}

SELECT 
    HOST_ID,
    REPLACE(HOST_NAME, ' ', '_') as HOST_NAME,
    HOST_SINCE,
    IS_SUPERHOST,
    CASE 
        WHEN RESPONSE_RATE > 95 THEN 'Excellent'
        WHEN RESPONSE_RATE > 80 THEN 'Good'
        WHEN RESPONSE_RATE > 60 THEN 'Average'
        ELSE 'Needs Improvement'
    END as RESPONSE_RATE_QUALITY,
    CREATED_AT
FROM {{ source('staging', 'hosts') }}
{% if is_incremental() %}
    WHERE CREATED_AT > (SELECT COALESCE(MAX(CREATED_AT), '1970-01-01') FROM {{ this }})
{% endif %}