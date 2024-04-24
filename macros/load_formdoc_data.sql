{% macro load_formdoc_data(table) %}

{% set query %}
    SELECT distinct jsonb_object_keys(form) FROM 
    (
    select  cast(_airbyte_data->>'formDoc' as jsonb) as form from  {{source('airbyte_raw', '_airbyte_raw_tickets')}}
    limit 100
    ) a
{% endset %}

{% set all_keys = run_query(query) %}
{% if execute %}
{% set keys = all_keys.columns[0].values() %}

{%- set unique_keys = [] -%}
{% for key in keys %}
    {%- do unique_keys.append(key) -%}
{%endfor%}
{{ print(unique_keys) }}
{% endif %}
{{print("-------------------------------------------")}}

{%set no_of_keys = unique_keys | length%}

{% if no_of_keys > 0 %}
    {% set query1 %}
        SELECT _airbyte_data->>'_id' as _id, cast(_airbyte_data->>'formDoc' as jsonb) as form, _airbyte_emitted_at  FROM {{source('airbyte_raw', '_airbyte_raw_tickets')}}
        limit 100

    {% endset %}

    {% set af = run_query(query1) %}
    {% if execute %}
    {% set fid = af.columns[0].values() %}
    {% set f = af.columns[1].values() %}
    {% set _airbyte_emitted_at = af.columns[2].values() %}
    {%endif%}

    {% set row_count = f | length %}

    {% set new_form = [] %}
    {%- for i in range(row_count) -%}
        {% set avail_form = fromjson(f[i]) %}
        {% set form = {} %}
        {% for key in unique_keys %}
            {% set value = avail_form[key] %}
            {% set value = value | string %}
            {% set value =  "'" ~ value ~ "'" %}
            {% if key not in f[i] %}
                {% do form.update({ key: "null" }) %}
            {%else%}
                {% do form.update({ key: value }) %}
            {%endif%}
        {%endfor%}
        {%- do new_form.append(form) -%}
    {%endfor%}

    {{print("------------------55-------------------------")}}

    {% set final_query = ['DROP TABLE IF EXISTS nithin.nivea.new_formdoc_table; ','create table nithin.nivea.new_formdoc_table (_id varchar, formdoc varchar, _airbyte_emitted_at varchar); ','insert into nithin.nivea.new_formdoc_table ("_id", "formdoc", "_airbyte_emitted_at") values '] %}

    {%- for i in range(new_form | length) -%}
        {% set id = fid[i] | string %}
        {% set nf = new_form[i] | string %}
        {% set nf =  nf | replace("'", '"') %}
        {% set nf =  nf | replace("Undefined", '"Undefined"') %}
        {% set _airbyte_emitted_at = _airbyte_emitted_at[i] | string %}
        {% set _airbyte_emitted_at = "'" ~ _airbyte_emitted_at ~ "'" %}

        {% set values_str = "( "  ~  "'"  ~  id  ~  "'"  ~  ","  ~  "'"  ~  nf  ~  "'"  ~  ","  ~  _airbyte_emitted_at ~  ")" %}
        {% if not loop.last %}
            {% set values_str = values_str ~ "," %}
        {% endif %}

        {%- do final_query.append(values_str) -%}
        
    {% endfor %}
    {%- do final_query.append(";") -%}


    {{ print(final_query)}}

    {{print("-------------------------90------------------")}}

    {% set query =  final_query | join(' ') %}

    {% do run_query(query) %}

    {{print("---------Query ran successfully---------")}}

{%else%}
    {{print("No new row added to table")}}
    {% set final_query = ['DROP TABLE IF EXISTS nithin.nivea.new_formdoc_table; ','create table nithin.nivea.new_formdoc_table (_id varchar, formdoc varchar , _airbyte_emitted_at varchar); '] %}
    {% set query =  final_query | join(' ') %}
    {% do run_query(query) %}
{%endif%}

{% endmacro %}