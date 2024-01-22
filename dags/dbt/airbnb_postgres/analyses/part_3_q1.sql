WITH
  census_1 AS
    (SELECT
      lga_code,
      SUM(
        age_0_4_yr_p
        + age_5_14_yr_p
        + age_15_19_yr_p
        + age_20_24_yr_p
        + age_25_34_yr_p
      ) AS age_under_34_population,
      SUM(
        age_0_4_yr_p
        + age_5_14_yr_p
        + age_15_19_yr_p
        + age_20_24_yr_p
        + age_25_34_yr_p
        + age_35_44_yr_p
        + age_45_54_yr_p
        + age_55_64_yr_p
        + age_65_74_yr_p
        + age_75_84_yr_p
        + age_85ov_p
      ) AS total_population
    FROM "postgres"."warehouse"."dim_census_1"
    WHERE dbt_valid_to IS NULL
    GROUP BY
      lga_code)

  ,census_2 AS
    (SELECT
      lga_code,
      median_age_persons
    FROM "postgres"."warehouse"."dim_census_2"
    WHERE dbt_valid_to IS NULL)

  ,dim_lga AS
    (SELECT
      lga_code,
      lga_name
    FROM "postgres"."warehouse"."dim_lga"
    WHERE dbt_valid_to IS NULL)

  ,facts_listings AS
    (SELECT
      *,
      CASE WHEN has_availability = 't' THEN listing_id END AS active_listing_id,
      CASE WHEN has_availability = 't' THEN price END AS active_listing_price,
      CASE WHEN has_availability = 't' THEN 30 - availability_30 END AS number_of_stays
    FROM "postgres"."warehouse"."facts_listings")

  ,agg AS
    (SELECT
      listing_neighbourhood_lga,
      (
        SUM(active_listing_price * number_of_stays)
        / COUNT(DISTINCT active_listing_id)
      ) AS estimated_revenue_per_active_listings
    FROM facts_listings
    GROUP BY
      listing_neighbourhood_lga)

  ,final AS
    (SELECT
      a.*,
      c1.age_under_34_population*100/total_population AS age_under_34_percent,
      c2.median_age_persons,
      RANK() OVER(ORDER BY estimated_revenue_per_active_listings DESC) AS ranking
    FROM agg AS a
    LEFT JOIN dim_lga AS dl
    ON a.listing_neighbourhood_lga = dl.lga_name
    LEFT JOIN census_1 AS c1
    ON dl.lga_code = c1.lga_code
    LEFT JOIN census_2 AS c2
    ON dl.lga_code = c2.lga_code)
  
  SELECT * FROM final
  WHERE ranking in (1, 29)
  ORDER BY ranking
