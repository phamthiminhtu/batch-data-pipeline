{# refer to macros/handle_dbt_valid_from.sql for more details #}

{{
  handle_dbt_valid_from(
    source=ref('property_snapshot'),
    columns_to_select=[
      'listing_id',
      'UPPER(listing_neighbourhood) AS listing_neighbourhood_lga',
      'property_type',
      'room_type',
      'accommodates',
      'updated_at'
    ])
}}