{% macro current_timestring(date_fmt='%Y%m%d%H%M%S%f') %}
    {% set dt = modules.datetime.datetime.now() %}
    {% do return(dt.strftime(date_fmt)) %}
{% endmacro %}

{# Return all variables from airlow pass down #}
{% macro get_vars_from_airflow() %}
	{%- set date_fmt = '%Y-%m-%dT%H:%M:%S+00:00' -%}
	{%- set airflow_vars = namespace() -%}
	{%- set airflow_vars.ds_nodash = var('ds_nodash', current_timestring('%Y%m%d')) -%}
	{%- set airflow_vars.ds_date = convert_datetime(var('ds_date', current_timestring(date_fmt)), date_fmt) -%}
	{%- do return(airflow_vars) -%}
{% endmacro %}
