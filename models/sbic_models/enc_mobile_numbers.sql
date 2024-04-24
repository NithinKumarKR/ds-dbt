-- Model Mobile Number

{{ config(
    materialized='incremental',
    unique_key = '_id',
    incremental_stategy = 'merge',
    on_schema_change= 'append_new_columns'
) }}


WITH transformed_data AS (
    SELECT
        *,
        mobile_number->>'mac' AS "mobile_number_mac",
        mobile_number->>'encryptedData' AS "mobile_number_encryptedData"

    FROM  {{source('airbyte_raw','mobile_numbers')}}

        {% if is_incremental() %}

        where TO_TIMESTAMP(SUBSTRING(cast(_airbyte_emitted_at as varchar), 1,23),'YYYY-MM-DD HH24:MI:SS.MS') > 
        (select max(TO_TIMESTAMP(SUBSTRING(cast(_airbyte_emitted_at as varchar), 1,23),'YYYY-MM-DD HH24:MI:SS.MS'))
        from {{this}})

        {% endif %}
)


SELECT * FROM transformed_data