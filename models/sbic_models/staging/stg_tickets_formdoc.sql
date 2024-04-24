

-- models/stg_tickets_formdoc.sql

{{ config(
    materialized='incremental',
    unique_key = '_id',
    incremental_stategy = 'merge',
    on_schema_change= 'append_new_columns'
) }}


WITH transformed_data AS (
    SELECT 
        _airbyte_data->>'_id' as _id,
        _airbyte_data->>'formDoc' as "formDoc",
        _airbyte_emitted_at

    FROM{{source('airbyte_raw','_airbyte_raw_tickets')}}

        {% if is_incremental() %}

        where TO_TIMESTAMP(SUBSTRING(cast(_airbyte_emitted_at as varchar), 1,23),'YYYY-MM-DD HH24:MI:SS.MS') > 
        (select max(TO_TIMESTAMP(SUBSTRING(cast(_airbyte_emitted_at as varchar), 1,23),'YYYY-MM-DD HH24:MI:SS.MS'))
        from {{this}} )

        {% endif %}
)


SELECT * FROM transformed_data