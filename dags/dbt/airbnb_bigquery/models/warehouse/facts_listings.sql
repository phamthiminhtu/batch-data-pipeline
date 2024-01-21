{# Get vars from airflow #}
{% set model_params = get_vars_from_airflow() %}

{#- define number of dates to back fill data based on running hour -#}
{% set result = get_dynamic_look_back_in_day(model_params) %}

{#- update model_params with useful date variables -#}
{% set result = get_common_model_params(model_params) %}

{{ config(
		partition_by={
			'field': 'scraped_date',
			'data_type': 'date',
		},
		materialized='incremental',
		incremental_strategy='insert_overwrite',
		partitions=model_params.partitions_quoted
)
}}

WITH

	dim_host AS
		(SELECT * FROM {{ ref('dim_host') }})

	,dim_suburb AS
		(SELECT * FROM {{ ref('dim_suburb') }})

	,dim_property AS
		(SELECT * FROM {{ ref('dim_property') }})

	,listings_stg AS
		(SELECT * FROM {{ ref("listings_stg") }}
		{% if is_incremental() %}
			WHERE scraped_date IN ( {{ model_params.partitions_quoted | join(',') }})
		{% endif %}
		)

	SELECT
		l.*,
		dh.host_is_superhost,
		dh.host_neighbourhood,
		dp.room_type,
		dp.property_type,
		dp.accommodates,
		dp.listing_neighbourhood_lga,
		ds.lga_name AS host_neighbourhood_lga
	FROM listings_stg AS l
	LEFT JOIN dim_property AS dp
	ON l.listing_id = dp.listing_id AND l.scraped_date >= dp.dbt_valid_from AND l.scraped_date < COALESCE(dp.dbt_valid_to, TIMESTAMP('9999-01-01'))
	LEFT JOIN dim_host AS dh
	ON l.host_id = dh.host_id AND l.scraped_date >= dh.dbt_valid_from AND l.scraped_date < COALESCE(dh.dbt_valid_to, TIMESTAMP('9999-01-01'))
	LEFT JOIN dim_suburb AS ds
	ON dh.host_neighbourhood = ds.suburb_name AND l.scraped_date >= ds.dbt_valid_from AND l.scraped_date < COALESCE(ds.dbt_valid_to, TIMESTAMP('9999-01-01'))