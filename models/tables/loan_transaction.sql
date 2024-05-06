
{{ config(
        materialized='table',
        table='loan_transaction',
        scheme='acno',
    on_schema_change='append_new_columns',
    merge_update='update',
) }}

-- Fetch unique keys
{% set query %}
    SELECT  
    DISTINCT jsonb_object_keys(
jsonb_array_elements(jsonb_extract_path(_airbyte_data::jsonb, 'transactionDetails')))
    FROM "garnier"."acno"."loan_flat_date"
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


SELECT
cast(jsonb_extract_path(
                _airbyte_data::jsonb, 
                    'loanId'
                        ) as varchar)  as loanId,
    {% for i in range(unique_keys | length) %}
        jsonb_array_elements(
            jsonb_extract_path(
                _airbyte_data::jsonb, 
                    'transactionDetails'
                        ))->>'{{unique_keys[i]}}' as {{unique_keys[i]}},
    {% endfor %}
      now() as dbt_date

FROM "garnier"."acno"."loan_flat_date"
group by 
    {% for i in range(unique_keys | length) %}
        jsonb_array_elements(
            jsonb_extract_path(
                _airbyte_data::jsonb, 
                    'transactionDetails'
                        ))->>'{{unique_keys[i]}}' ,
    {% endfor %}
cast(jsonb_extract_path(
                _airbyte_data::jsonb, 
                    'loanId'
                        ) as varchar) 
order by 8