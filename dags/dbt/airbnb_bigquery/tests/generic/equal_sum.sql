{% test equal_sum(model, compare_to, column_1, column_2) -%}
    WITH
      ref_1 AS
        (SELECT SUM({{ column_1 }}) AS sum_1 FROM {{ model }})

      ,ref_2 AS
        (SELECT  SUM({{ column_2 }}) AS sum_2 FROM {{ compare_to }})

      SELECT
        *
      FROM ref_1
      CROSS JOIN ref_2
      WHERE sum_1 != sum_2
{% endtest %}