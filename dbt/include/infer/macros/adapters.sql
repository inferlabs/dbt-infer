
{% macro infer__current_timestamp() -%}
'''Returns current UTC time'''
{# docs show not to be implemented currently. #}
{% endmacro %}

{% macro infer__create_view_as(relation, sql) -%}
    {% do adapter.set_create_view_mode(True) %}
    {% do return(adapter.adapter_macro('create_view_as', {'relation': relation, 'sql': '(' + sql + ')'})) %}
{% endmacro %}

{% macro infer__create_table_as(temporary, relation, compiled_code, language='sql') -%}
    {% do adapter.set_create_view_mode(False) %}
    {% do return(adapter.adapter_macro(
        'create_table_as',
        {'temporary': temporary, 'relation': relation, 'compiled_code': compiled_code, 'language': language}))
    %}
{% endmacro %}

{% macro infer__generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}
        {{ node.schema }}
    {%- else -%}
        {{ default_schema }}_{{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
