{{ config(
    materialized='incremental',
    unique_key = '_id',
    incremental_stategy = 'merge',
    on_schema_change= 'append_new_columns'
) }}

-- Got unique keys for one day
{% set query %}
    SELECT distinct jsonb_object_keys(cast(_airbyte_data->>'formDoc' as jsonb)) FROM {{source('airbyte_raw', '_airbyte_raw_tickets')}}
        where TO_TIMESTAMP(SUBSTRING(cast(_airbyte_emitted_at as varchar), 1,23),'YYYY-MM-DD HH24:MI:SS.MS') > 
        (select max(TO_TIMESTAMP(SUBSTRING(cast(_airbyte_emitted_at as varchar), 1,23),'YYYY-MM-DD HH24:MI:SS.MS'))
        from {{this}} )

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
    {%else%}
        {%- do unique_keys.append(key) -%}
        {%- do lower_unique_keys.append(key_new) -%}
    {%endif%}
{%endfor%}
{{ print(unique_keys) }}
{% endif %}
{{print("-------------------------------------------")}}

select _id , _airbyte_ab_id
{% for i in range(unique_keys | length) %}
    ,
    formdoc->>'{{unique_keys[i]}}' as {{unique_keys[i]}}
{%endfor%}
    ,
    _airbyte_emitted_at
from
    (select _airbyte_ab_id, _airbyte_data->>'_id' as _id , cast(_airbyte_data->>'formDoc' as jsonb) as formdoc,
    TO_TIMESTAMP(SUBSTRING(cast(_airbyte_emitted_at as varchar), 1,23),'YYYY-MM-DD HH24:MI:SS.US') as _airbyte_emitted_at
    from {{source('airbyte_raw', '_airbyte_raw_tickets')}}

    where TO_TIMESTAMP(SUBSTRING(cast(_airbyte_emitted_at as varchar), 1,23),'YYYY-MM-DD HH24:MI:SS.MS') > 
        (select max(TO_TIMESTAMP(SUBSTRING(cast(_airbyte_emitted_at as varchar), 1,23),'YYYY-MM-DD HH24:MI:SS.MS'))
        from {{this}} )
    ) a