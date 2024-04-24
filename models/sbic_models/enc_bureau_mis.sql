-- models/user_data_transformed.sql

{{ config(
    materialized='incremental',
    unique_key = '_id',
    incremental_stategy = 'merge',
    on_schema_change= 'append_new_columns'
) }}


WITH transformed_data AS (

    select  * ,	pttelephone0->>'mac' AS "pttelephone0_mac"
            ,	pttelephone0->>'encryptedData' AS "pttelephone0_encryptedData"
            ,	pttelephone1->>'mac' AS "pttelephone1_mac"
            ,	pttelephone1->>'encryptedData' AS "pttelephone1_encryptedData"
            ,	pttelephone2->>'mac' AS "pttelephone2_mac"
            ,	pttelephone2->>'encryptedData' AS "pttelephone2_encryptedData"
            ,	pttelephone3->>'mac' AS "pttelephone3_mac"
            ,	pttelephone3->>'encryptedData' AS "pttelephone3_encryptedData"

    FROM{{source('airbyte_raw','bureau_mis')}} 

        {% if is_incremental() %}

        where TO_TIMESTAMP(SUBSTRING(cast(_airbyte_emitted_at as varchar), 1,23),'YYYY-MM-DD HH24:MI:SS.MS') > 
        (select max(TO_TIMESTAMP(SUBSTRING(cast(_airbyte_emitted_at as varchar), 1,23),'YYYY-MM-DD HH24:MI:SS.MS'))
        from {{this}} )

        {% endif %}
)


SELECT * FROM transformed_data