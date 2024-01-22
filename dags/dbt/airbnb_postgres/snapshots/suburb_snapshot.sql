{% snapshot suburb_snapshot %}

{{ config(
  strategy="timestamp",
  updated_at="ingestion_timestamp",
  unique_key="suburb_name"
) }}

SELECT * FROM {{ source('airbnb_raw', 'nsw_lga_suburb') }}
{% endsnapshot %}