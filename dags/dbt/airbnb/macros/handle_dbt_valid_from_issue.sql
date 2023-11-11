{#-
  This macro handles cases when the snapshot tables are created 
  after the data was generated.
  Modularize the logic into a macro to reduce duplicated code.
-#}

{% macro handle_dbt_valid_from(source, columns_to_select) -%}
  WITH
    source_data AS
      (SELECT
        *
      FROM {{ source }})

    ,get_min_dbt_valid_from AS
      (SELECT
        MIN(dbt_valid_from) AS min_dbt_valid_from
      FROM source_data)

    SELECT
      {{ columns_to_select | join(',\n')}},
      dbt_scd_id,
      dbt_updated_at,
      -- handle cases when snapshot tables are created after the data was generated
      CASE WHEN DATE(dbt_valid_from) = DATE(m.min_dbt_valid_from) THEN '1111-01-01'::TIMESTAMP ELSE dbt_valid_from END AS dbt_valid_from,
      dbt_valid_to
    FROM source_data AS s
    CROSS JOIN get_min_dbt_valid_from AS m

{%- endmacro %}