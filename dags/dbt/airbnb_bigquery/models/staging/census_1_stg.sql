{# refer to macros/handle_dbt_valid_from.sql for more details #}

{{
  handle_dbt_valid_from(
    source=ref('census_1_snapshot'),
    columns_to_select=[
      "REPLACE(lga_code_2016, 'LGA', '') AS lga_code",
      'age_0_4_yr_p',
      'age_5_14_yr_p',
      'age_15_19_yr_p',
      'age_20_24_yr_p',
      'age_25_34_yr_p',
      'age_35_44_yr_p',
      'age_45_54_yr_p',
      'age_55_64_yr_p',
      'age_65_74_yr_p',
      'age_75_84_yr_p',
      'age_85ov_p'
    ])
}}