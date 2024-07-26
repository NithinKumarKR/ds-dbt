{{ config(
    materialized='incremental',
    unique_key = 'pkey',
    incremental_stategy = 'merge',
    on_schema_change= 'append_new_columns'
) }}
-- Fetch unique keys from the last 3 months

SELECT * 
FROM "mac"."dw"."dw_core_accounts"
WHERE date(last_modified_date) >= date(CURRENT_DATE - interval '3 months')
and  TO_TIMESTAMP(SUBSTRING(cast(_airbyte_extracted_at as varchar), 1,23),'YYYY-MM-DD HH24:MI:SS.MS') > 
        COALESCE((select max(TO_TIMESTAMP(SUBSTRING(cast(_airbyte_extracted_at as varchar), 1,23),'YYYY-MM-DD HH24:MI:SS.MS'))
        from {{this}} ),'10000-07-19 07:40:24.709+00')
        
