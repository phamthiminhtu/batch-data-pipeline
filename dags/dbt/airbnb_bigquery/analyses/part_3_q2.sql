WITH
  facts_listings AS
    (SELECT
      *,
      CASE WHEN has_availability = 't' THEN listing_id END AS active_listing_id,
      CASE WHEN has_availability = 't' THEN price END AS active_listing_price,
      CASE WHEN has_availability = 't' THEN 30 - availability_30 END AS number_of_stays
    FROM "postgres"."warehouse"."facts_listings")

  ,get_top_5_neighbourhood AS
    (SELECT
      listing_neighbourhood_lga,
      (
        SUM(active_listing_price * number_of_stays)
        / COUNT(DISTINCT active_listing_id)
      ) AS estimated_revenue_per_active_listings
    FROM facts_listings
    GROUP BY
      listing_neighbourhood_lga
    ORDER BY estimated_revenue_per_active_listings DESC
    LIMIT 5)

  ,agg AS
    (SELECT
      t.listing_neighbourhood_lga,
      property_type,
      room_type,
      accommodates,
      SUM(number_of_stays) AS number_of_stays
    FROM facts_listings AS f
    LEFT JOIN get_top_5_neighbourhood AS t
    ON f.listing_neighbourhood_lga = t.listing_neighbourhood_lga
    WHERE
      t.listing_neighbourhood_lga IS NOT NULL -- get top 5 listing_neighbourhood_lga
    GROUP BY
      t.listing_neighbourhood_lga,
      property_type,
      room_type,
      accommodates
    )

  ,get_rank AS
    (SELECT
      *,
      RANK() OVER(PARTITION BY listing_neighbourhood_lga ORDER BY number_of_stays DESC) AS number_of_stays_ranking
    FROM agg)
  
  SELECT * FROM get_rank
  WHERE number_of_stays_ranking = 1