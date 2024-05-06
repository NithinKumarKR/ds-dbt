{{ config(
        materialized='table',
    unique_key='pkey',
    on_schema_change='append_new_columns',
    merge_update='update',
     incremental_stategy = 'merge'

) }}

-- Fetch unique keys
{% set query %}
    SELECT DISTINCT jsonb_object_keys(cast(_airbyte_data as jsonb))
    FROM "garnier"."acno"."buckets"
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
{{ print('table_keys') }}
{{ print(unique_keys) }}
{% endif %}

{{ print("--------------------------------------------------------------------------------------") }}

-- Inserting new records

SELECT



    {% for i in range(unique_keys | length) %}
        cast(_airbyte_data as jsonb)->>'{{unique_keys[i]}}' as {{unique_keys[i]}},
    {% endfor %}

    _airbyte_ab_id,
    cast(_airbyte_data as jsonb),
    now() as dbt_date 

FROM "garnier"."acno"."buckets"


