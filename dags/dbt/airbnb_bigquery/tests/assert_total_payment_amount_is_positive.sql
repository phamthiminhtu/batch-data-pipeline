-- Refunds have a negative amount, so the total amount should always be >= 0.
-- Therefore return records where this isn't true to make the test fail
SELECT
  listing_id,
  SUM(price) as price
FROM {{ ref('facts_listings' )}}
GROUP BY listing_id
HAVING NOT(price >= 0)