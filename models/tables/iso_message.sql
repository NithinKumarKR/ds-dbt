{{ config(
    materialized='incremental',
    unique_key = '_id',
    incremental_stategy = 'merge',
    on_schema_change= 'append_new_columns'
) }}

-- Fetch unique keys
{% set query %}
    SELECT DISTINCT jsonb_object_keys(cast(_airbyte_data as jsonb))
    FROM "indusind_reports"."dw"."_airbyte_raw_indmillennial_iso_message"
{% endset %}

{% set all_keys = run_query(query) %}
{% if execute %}
{% set keys = all_keys.columns[0].values() %}

{%- set unique_keys = [] -%}
{%- set lower_unique_keys = [] -%}
{%- set count = 1 -%}
{% for key in keys %}
    {% set key_new = key | lower %}
    {% if key_new in lower_unique_keys %}
        {% set key = key ~ '_' ~ count %}
        {% set key_new = key_new ~ '_' ~ count %}
        {%- do unique_keys.append(key) -%}
        {%- do lower_unique_keys.append(key_new) -%}
        {%- set count = count + 1 -%}
    {% else %}
        {%- do unique_keys.append(key) -%}
        {%- do lower_unique_keys.append(key_new) -%}
    {% endif %}
{% endfor %}


{{ print(unique_keys) }}
{% endif %}

{{ print("--------------------------------------------------------------------------------------") }}

-- Inserting new records
WITH transformed_data AS (

SELECT
        
    {% for key in unique_keys %}        
        cast(_airbyte_data as jsonb)->>'{{ key }}' as {{ key }},
    {% endfor %}
*
    FROM "indusind_reports"."dw"."_airbyte_raw_indmillennial_iso_message"

        {% if is_incremental() %}

        where TO_TIMESTAMP(SUBSTRING(cast(_airbyte_emitted_at as varchar), 1,23),'YYYY-MM-DD HH24:MI:SS.MS') > 
        COALESCE((select max(TO_TIMESTAMP(SUBSTRING(cast(_airbyte_emitted_at as varchar), 1,23),'YYYY-MM-DD HH24:MI:SS.MS'))
        from {{this}}),
        TO_TIMESTAMP(SUBSTRING(cast('1990-06-26 11:51:19.775' as varchar), 1,23),'YYYY-MM-DD HH24:MI:SS.MS'))
        {% endif %}
)


SELECT * FROM transformed_data
limit 100


