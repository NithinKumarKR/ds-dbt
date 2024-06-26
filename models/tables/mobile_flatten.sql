{{ config(
        materialized='table',
    unique_key='_id',
    on_schema_change='append_new_columns',
    merge_update='update',
) }}

-- Fetch unique keys
{% set query %}
    SELECT DISTINCT jsonb_object_keys(cast(_airbyte_data as jsonb))
    FROM "plus"."airbyte_internal"."nivea_raw__stream_mobile"
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
{%- do unique_keys.remove('_id') -%}
{%- do unique_keys.insert(0, '_id') -%}
{{ print('table_keys') }}
{{ print(unique_keys) }}
{% endif %}

{{ print("--------------------------------------------------------------------------------------") }}

-- Inserting new records

SELECT
    {% for i in range(unique_keys | length) %}
        cast(_airbyte_data as jsonb)->>'{{unique_keys[i]}}' as {{unique_keys[i]}},
    {% endfor %}
    
    replace(
        SPLIT_PART(cast(_airbyte_data as jsonb)->>'RGB', ',', 1),
        'rgb(','') 
         as x
        ,
        SPLIT_PART(cast(_airbyte_data as jsonb)->>'RGB', ',', 2)
         as y
        ,replace(
        SPLIT_PART(cast(_airbyte_data as jsonb)->>'RGB', ',', 3)
        ,')','')
                 as z

        ,
    _airbyte_raw_id,
    cast(_airbyte_data as jsonb),
    now() as dbt_date 

FROM "plus"."airbyte_internal"."nivea_raw__stream_mobile"
