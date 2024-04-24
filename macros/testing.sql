{% macro json_output(table_name, column_name) %}
    CAST("{{ table_name }}" AS JSON)->> '{{ column_name }}'
{% endmacro %}


{% macro json_output_2(table_name, column_name,column_name1) %}
COALESCE((
CAST( CAST("{{ table_name }}" AS JSON)->> '{{ column_name }}' AS JSON) ->> '{{ column_name1 }}'),
NULL ) 
{% endmacro %}

{% macro json_output_3(table_name, column_name,column_name1,column_name2) %}
COALESCE((
CAST(CAST( CAST("{{ table_name }}" AS JSON)->> '{{ column_name }}' AS JSON) ->> '{{ column_name1 }}')->> '{{ column_name2}}'),
NULL ) 
{% endmacro %}

{% macro json_keys(json_column) %}
    jsonb_object_keys({{ json_column }})
{% endmacro %}




{% macro json_extract(column_name, key) %}
    jsonb_extract_path_text("{{column_name}}"::jsonb, '{{ key }}')
{% endmacro %}
