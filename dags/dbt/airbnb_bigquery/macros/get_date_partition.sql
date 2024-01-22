{% macro get_date_partition(start_date_dt, end_date_dt) %}
	{%- set start_date_str_nodash = start_date_dt.strftime('%Y%m%d') -%}
	{%- set end_date_str_nodash = end_date_dt.strftime('%Y%m%d') -%}
	{%- set partitions_quoted = dates_in_range(
			start_date_str_nodash,
			end_date_str_nodash,
			out_fmt="'%Y-%m-%d'"
		)
	-%}
	{%- do return(partitions_quoted) -%}
{% endmacro %}