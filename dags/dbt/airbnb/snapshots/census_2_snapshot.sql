{% snapshot census_2_snapshot %}

{{ config(
  strategy="timestamp",
  updated_at="ingestion_timestamp",
  unique_key="lga_code_2016"
) }}

SELECT * FROM {{ source('airbnb_raw', 'census_lga_g02') }}
{% endsnapshot %}