
{# model_params = namespace containing at least ds and ds_date and lookback_in_days #}
{% macro get_common_model_params(model_params) %}
	{%- set model_params.start_date_dt = model_params.ds_date + modules.datetime.timedelta(days=-model_params.lookback_in_days) -%}
	{%- set model_params.partitions_quoted = get_date_partition(start_date_dt=model_params.start_date_dt, end_date_dt=model_params.ds_date) -%}
{% endmacro %}