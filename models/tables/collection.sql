{{ config(
    materialized='table',
    unique_key='_id',
    on_schema_change='append_new_columns',
    merge_update='update',

) }}

-- Fetch unique keys
{% set query %}
    SELECT DISTINCT jsonb_object_keys(cast(_airbyte_data as jsonb))
    FROM "garnier"."acno"."acno_raw__stream_cello_collection"
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
{%- do unique_keys.remove('person') -%}
{{ print('table_keys') }}
{{ print(unique_keys) }}
{% endif %}

{{ print("--------------------------------------------------------------------------------------") }}




{% set query %}
    SELECT DISTINCT jsonb_object_keys(cast(cast(_airbyte_data as jsonb)->>'person' as jsonb))
    FROM "garnier"."acno"."acno_raw__stream_cello_collection"
{% endset %}

{% set all_keys = run_query(query) %}
{% if execute %}
{% set keys = all_keys.columns[0].values() %}

{%- set person_unique_keys = [] -%}
{%- set lower_unique_keys = [] -%}
{%- set count = 1 -%}
{% for key in keys %}
    {% set key_new = key | lower %}
    {% if key_new in lower_unique_keys %}
        {% set key = key ~ '_' ~ count %}
        {% set key_new = key_new ~ '_' ~ count %}
        {%- do person_unique_keys.append(key) -%}
        {%- do lower_unique_keys.append(key_new) -%}
        {%- set count = count + 1 -%}
    {% else %}
        {%- do person_unique_keys.append(key) -%}
        {%- do lower_unique_keys.append(key_new) -%}
    {% endif %}
{% endfor %}
{{ print('Person_keys') }}
{%- do person_unique_keys.remove('dates') -%}
{%- do person_unique_keys.remove('address') -%}
{{ print(person_unique_keys) }}

{% endif %}

{{ print("--------------------------------------------------------------------------------------") }}




{% set query %}
    SELECT DISTINCT jsonb_object_keys(cast(cast(cast(_airbyte_data as jsonb)->>'person' as jsonb)->>'address' as jsonb))
    FROM "garnier"."acno"."acno_raw__stream_cello_collection"
{% endset %}

{% set all_keys = run_query(query) %}
{% if execute %}
{% set keys = all_keys.columns[0].values() %}

{%- set address_unique_keys = [] -%}
{%- set lower_unique_keys = [] -%}
{%- set count = 1 -%}
{% for key in keys %}
    {% set key_new = key | lower %}
    {% if key_new in lower_unique_keys %}
        {% set key = key ~ '_' ~ count %}
        {% set key_new = key_new ~ '_' ~ count %}
        {%- do address_unique_keys.append(key) -%}
        {%- do lower_unique_keys.append(key_new) -%}
        {%- set count = count + 1 -%}
    {% else %}
        {%- do address_unique_keys.append(key) -%}
        {%- do lower_unique_keys.append(key_new) -%}
    {% endif %}
{% endfor %}
{{ print('address_keys') }}
{{ print(address_unique_keys) }}

{% endif %}

{{ print("--------------------------------------------------------------------------------------") }}


{% set query %}
    SELECT DISTINCT jsonb_object_keys(cast(cast(cast(_airbyte_data as jsonb)->>'person' as jsonb)->>'dates' as jsonb))
    FROM "garnier"."acno"."acno_raw__stream_cello_collection"
{% endset %}

{% set all_keys = run_query(query) %}
{% if execute %}
{% set keys = all_keys.columns[0].values() %}

{%- set dates_unique_keys = [] -%}
{%- set lower_unique_keys = [] -%}
{%- set count = 1 -%}
{% for key in keys %}
    {% set key_new = key | lower %}
    {% if key_new in lower_unique_keys %}
        {% set key = key ~ '_' ~ count %}
        {% set key_new = key_new ~ '_' ~ count %}
        {%- do dates_unique_keys.append(key) -%}
        {%- do lower_unique_keys.append(key_new) -%}
        {%- set count = count + 1 -%}
    {% else %}
        {%- do dates_unique_keys.append(key) -%}
        {%- do lower_unique_keys.append(key_new) -%}
    {% endif %}
{% endfor %}
{{ print('dates_keys') }}
{{ print(dates_unique_keys) }}

{% endif %}

{{ print("--------------------------------------------------------------------------------------") }}


-- Inserting new records

SELECT



    {% for i in range(unique_keys | length) %}
        cast(_airbyte_data as jsonb)->>'{{unique_keys[i]}}' as {{unique_keys[i]}},
    {% endfor %}

           {% for i in range(person_unique_keys | length) %}
        cast(cast(_airbyte_data as jsonb)->>'person' as jsonb)->>'{{person_unique_keys[i]}}'  as {{person_unique_keys[i]}} ,
    {% endfor %}

           {% for i in range(address_unique_keys | length) %}
        cast(cast(cast(_airbyte_data as jsonb)->>'person' as jsonb)->>'address' as jsonb)
        ->>'{{address_unique_keys[i]}}'  as {{address_unique_keys[i]}} ,
    {% endfor %}

           {% for i in range(dates_unique_keys | length) %}
        cast(cast(cast(_airbyte_data as jsonb)->>'person' as jsonb)->>'dates' as jsonb)
        ->>'{{dates_unique_keys[i]}}'  as {{dates_unique_keys[i]}} ,
    {% endfor %}
    _airbyte_raw_id,
    cast(_airbyte_data as jsonb),
    now() as dbt_date 

FROM "garnier"."acno"."acno_raw__stream_cello_collection"


