WITH
  facts_listings AS
    (SELECT * FROM "postgres"."warehouse"."facts_listings")
  
  ,dim_host AS
    (SELECT
      host_id,
      host_neighbourhood AS current_host_neighbourhood
    FROM "postgres"."warehouse"."dim_host"
    WHERE dbt_valid_to IS NULL)

  ,dim_suburb AS
    (SELECT
      lga_name,
      suburb_name
    FROM "postgres"."warehouse"."dim_suburb"
    WHERE dbt_valid_to IS NULL)

  ,get_host_listing AS
    (SELECT
      host_id,
      COUNT(DISTINCT listing_id) > 1 AS is_host_having_multiple_listing
    FROM facts_listings
    GROUP BY
      host_id)

  ,get_host_current_lga AS
    (SELECT
      dh.host_id,
      COALESCE(ds.lga_name, 'OUTSIDE_NSW') AS current_host_lga_name
    FROM dim_host AS dh
    LEFT JOIN dim_suburb AS ds
    ON dh.current_host_neighbourhood = ds.suburb_name)

  ,get_host_listings_lga AS
    (SELECT DISTINCT
      host_id,
      listing_neighbourhood_lga
    FROM facts_listings)

 ,get_host_listing_neighbourhood_info AS
  (SELECT
      ghll.host_id,
      MAX(
        CASE
          WHEN ghcl.current_host_lga_name = ghll.listing_neighbourhood_lga THEN 1
          ELSE 0
        END
      ) AS has_listing_in_neighbourhood
    FROM get_host_listings_lga AS ghll
    LEFT JOIN get_host_current_lga AS ghcl
    ON ghll.host_id = ghcl.host_id
    GROUP BY ghll.host_id)
  
  ,final AS
    (SELECT
      CASE WHEN ghlni.has_listing_in_neighbourhood = 1 THEN 'yes' ELSE 'no' END AS has_listing_in_neighbourhood,
      CASE WHEN ghl.is_host_having_multiple_listing THEN 'multiple' ELSE 'one' END AS number_of_listings_of_host,
      COUNT(ghlni.host_id) AS host_count
    FROM get_host_listing_neighbourhood_info AS ghlni
    LEFT JOIN get_host_listing AS ghl
    ON ghlni.host_id = ghl.host_id
    GROUP BY
      ghlni.has_listing_in_neighbourhood,
      number_of_listings_of_host)

  SELECT
    number_of_listings_of_host,
    has_listing_in_neighbourhood,
    (host_count/ SUM(host_count) OVER (PARTITION BY number_of_listings_of_host))::FLOAT*100 AS percentage_of_host
  FROM final
  ORDER BY number_of_listings_of_host