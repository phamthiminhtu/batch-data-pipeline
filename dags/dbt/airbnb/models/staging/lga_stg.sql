{# refer to macros/handle_dbt_valid_from.sql for more details #}

{{
  handle_dbt_valid_from(
    source=ref('lga_snapshot'),
    columns_to_select=[
      'lga_code',
      'UPPER(lga_name) AS lga_name'
    ])
}}