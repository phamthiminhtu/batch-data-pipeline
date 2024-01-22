{# refer to macros/handle_dbt_valid_from.sql for more details #}

{{ 
  handle_dbt_valid_from(
    source=ref('host_snapshot'),
    columns_to_select=[
        'host_id',
        'host_name',
        "PARSE_DATE('DD/MM/YYYY', host_since) AS host_since",
        'host_is_superhost',
        'UPPER(TRIM(host_neighbourhood)) AS host_neighbourhood',
        'updated_at'
    ])
}}