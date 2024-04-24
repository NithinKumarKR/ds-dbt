{{ config(
    materialized='incremental',
    unique_key = '_id',
    incremental_stategy = 'merge',
    on_schema_change= 'append_new_columns'
) }}


WITH transformed_data AS (
    SELECT
        dob->>'mac' AS dob_mac,
        dob->>'encryptedData' AS "dob_encryptedData",
        gender->>'mac' AS gender_mac,
        gender->>'encryptedData' AS "gender_encryptedData"

    FROM  {{source('airbyte_raw','aadhar_information')}}

        {% if is_incremental() %}

        where TO_TIMESTAMP(SUBSTRING(cast(_airbyte_emitted_at as varchar), 1,23),'YYYY-MM-DD HH24:MI:SS.MS') > 
        (select max(TO_TIMESTAMP(SUBSTRING(cast(_airbyte_emitted_at as varchar), 1,23),'YYYY-MM-DD HH24:MI:SS.MS'))
        from {{this}})

        {% endif %}
)

SELECT * FROM transformed_data