{# Get vars from airflow #}
{% set model_params = get_vars_from_airflow() %}

{{ config(
  strategy="timestamp",
  updated_at="updated_at",
  unique_key="host_id",
) }}

WITH
  source AS
    (SELECT
      *,
      ROW_NUMBER() OVER(PARTITION BY host_id ORDER BY scraped_date DESC) AS _row_number
    FROM {{ source('airbnb_raw', 'listings') }}
    -- only run 1 date of data to backfill data in the past
    WHERE scraped_date = '{{ model_params.ds_date }}')

  SELECT
    host_id,
    host_name,
    host_since,
    host_is_superhost,
    host_neighbourhood,
    scraped_date::TIMESTAMP AS updated_at
  FROM source
  -- get the one unique record in case the unique key has different values within 1 scraped_date
  -- e.g.: host has 2 listing ids with different host_neighbourhood
  WHERE _row_number = 1
{% endsnapshot %}