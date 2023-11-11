{#-
	This model uses the "delete+insert" (i.e. insert overwrite) strategy
	using the <run_date> and <interval> (interval = 2 by default) variables:
		Because postgres does not support "insert overwrite" strategy, we use
		the "delete+insert" strategy instead:
		- dbt will delete data with scraped_date in the [run_date - 2,  run_date] range
		and insert new data of that date range.
		- The config `unique_key='scraped_date'` is used for this purpose, 
		although scraped_date is not the unique_key of the table. This setting is equal
		to the "insert overwrite" strategy in other databases.

	A few ways to define run_date variable:
		1. Define the run_date (and interval) in dbt_profiles.yml (default)
		2. Override run_date (and interval) in dbt_profiles.yml by passing vars using
			cli. e.g: `dbt run --vars '{"run_date": "2023-10-01"}'`
		3. Run with run_date=today by deleting the run_date in dbt_profiles.yml.
-#}
{%- set run_date = var('run_date', modules.datetime.datetime.today().strftime("%Y-%m-%d")) -%}
{%- set interval = var('interval') -%}

{{ config(
		partition_by=['scraped_date'],
		unique_key='scraped_date',
		partition_type="date",
		materialized='incremental',
		incremental_strategy='delete+insert',
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
		-- insert overwrite 3 day data: from run_date - 2 to run_date
		{% if is_incremental() %}
			WHERE scraped_date BETWEEN ('{{ run_date }}'::DATE - INTERVAL '{{ interval }} DAY')::DATE AND ('{{ run_date }}')::DATE
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
	ON l.listing_id = dp.listing_id AND l.scraped_date >= dp.dbt_valid_from AND l.scraped_date < COALESCE(dp.dbt_valid_to, '9999-01-01'::TIMESTAMP)
	LEFT JOIN dim_host AS dh
	ON l.host_id = dh.host_id AND l.scraped_date >= dh.dbt_valid_from AND l.scraped_date < COALESCE(dh.dbt_valid_to, '9999-01-01'::TIMESTAMP)
	LEFT JOIN dim_suburb AS ds
	ON dh.host_neighbourhood = ds.suburb_name AND l.scraped_date >= ds.dbt_valid_from AND l.scraped_date < COALESCE(ds.dbt_valid_to, '9999-01-01'::TIMESTAMP)