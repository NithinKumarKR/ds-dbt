-- models/user_data_transformed.sql

{{ config(
    materialized='incremental',
    unique_key = '_id',
    incremental_stategy = 'merge',
    on_schema_change= 'append_new_columns'
) }}


WITH transformed_data AS (
    SELECT
        *,
        email_address->>'mac' AS "email_address_mac",
        email_address->>'encryptedData' AS "email_address_encryptedData"

    FROM  {{source('airbyte_raw','email_addresses')}}

        {% if is_incremental() %}

        where TO_TIMESTAMP(SUBSTRING(cast(_airbyte_emitted_at as varchar), 1,23),'YYYY-MM-DD HH24:MI:SS.MS') > 
        (select max(TO_TIMESTAMP(SUBSTRING(cast(_airbyte_emitted_at as varchar), 1,23),'YYYY-MM-DD HH24:MI:SS.MS'))
        -- from dbt.dbt_trans.transformed_email_address tea)
        from {{this}}
        -- dbt_transformation.user_data_transformed
        )
        
        {% endif %}
)


SELECT * FROM transformed_data