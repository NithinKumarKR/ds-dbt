{{ config(
        materialized='table',
    unique_key='loanId',
    on_schema_change='append_new_columns',
    merge_update='update',
) }}

-- Fetch unique keys
{% set query %}
    SELECT DISTINCT jsonb_object_keys(cast(_airbyte_data as jsonb))
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
{%- do unique_keys.remove('loanId') -%}
{%- do unique_keys.insert(0, 'loanId') -%}

{{ print(unique_keys) }}
{% endif %}

{{ print("--------------------------------------------------------------------------------------") }}



-- Inserting new records

SELECT
    _airbyte_ab_id,
    cast(_airbyte_data as jsonb),
    {% for i in range(unique_keys | length) %}
        cast(_airbyte_data as jsonb)->>'{{unique_keys[i]}}' as {{unique_keys[i]}},
    {% endfor %}
    TO_TIMESTAMP(SUBSTRING(cast(_airbyte_emitted_at as varchar), 1, 23), 'YYYY-MM-DD HH24:MI:SS.US') as _airbyte_emitted_at,
      now() as dbt_date

FROM "garnier"."acno"."loan_flat_date"


