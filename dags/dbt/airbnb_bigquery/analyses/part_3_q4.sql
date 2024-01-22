WITH

  census AS
    (SELECT
      lga_code,
      median_mortgage_repay_monthly*12 AS annualised_median_mortgage_repay
    FROM "postgres"."warehouse"."dim_census_2")

  ,facts_listings AS
    (SELECT
      *,
      CASE WHEN has_availability = 't' THEN price END AS active_listing_price,
      CASE WHEN has_availability = 't' THEN 30 - availability_30 END AS number_of_stays
    FROM "postgres"."warehouse"."facts_listings")

  ,dim_lga AS
    (SELECT
      lga_code,
      lga_name
    FROM "postgres"."warehouse"."dim_lga"
    WHERE dbt_valid_to IS NULL)

  ,get_host_listing AS
    (SELECT
      host_id,
      COUNT(DISTINCT listing_id) = 1 AS is_host_having_one_listing
    FROM facts_listings
    GROUP BY
      host_id)
    
  ,get_host_with_one_listing_performance AS
    (SELECT
      fl.host_id,
      dl.lga_code,
      SUM(fl.number_of_stays * fl.active_listing_price) AS estimated_revenue_last_12_months
    FROM facts_listings AS fl
    LEFT JOIN get_host_listing AS ghl
    ON fl.host_id = ghl.host_id
    LEFT JOIN dim_lga AS dl
    ON fl.listing_neighbourhood_lga = dl.lga_name
    -- filter only hosts having 1 listing
    WHERE ghl.is_host_having_one_listing
    GROUP BY
      fl.host_id,
      lga_code)

  ,compare_with_listing_mortgage_repayment AS
    (SELECT
      ghwolp.estimated_revenue_last_12_months >= c.annualised_median_mortgage_repay AS has_median_mortgage_repay_annualised_covered,
      COUNT(ghwolp.host_id) AS host_count
    FROM get_host_with_one_listing_performance AS ghwolp
    LEFT JOIN census AS c
    ON ghwolp.lga_code = c.lga_code
    GROUP BY has_median_mortgage_repay_annualised_covered)

  SELECT
    CASE WHEN has_median_mortgage_repay_annualised_covered THEN 'yes' ELSE 'no' END AS has_median_mortgage_repay_annualised_covered,
    (host_count*100/SUM(host_count) OVER())::FLOAT AS percent
  FROM compare_with_listing_mortgage_repayment