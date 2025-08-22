{% macro get_columns_without_prefix(source_name, table_name, prefix) %}
    {% set cols = adapter.get_columns_in_relation(source(source_name, table_name)) %}
    {% set select_list = [] %}

    {% set upper_prefix = prefix|string|upper %}
    {% set prefix_length = (prefix|string)|length %}

    {% for col in cols %}
        {% set col_name = col.name|string %}
        {% set slice_start = prefix_length|int %}
        {% if col.name.startswith(upper_prefix) %}
            {% do select_list.append(col.name ~ ' AS ' ~ col.name[slice_start:]) %}
        {% else %}
            {% do select_list.append(col.name) %}
        {% endif %}
    {% endfor %}

    {{ return(select_list | join(',\n    ')) }}
{% endmacro %}