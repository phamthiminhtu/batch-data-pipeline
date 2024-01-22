{% snapshot lga_snapshot %}

{{ config(
  strategy="timestamp",
  updated_at="ingestion_timestamp",
  unique_key="lga_code"
) }}

SELECT * FROM {{ source('airbnb_raw', 'nsw_lga_code') }}
{% endsnapshot %}