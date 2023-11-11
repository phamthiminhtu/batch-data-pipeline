
WITH
	listings_source AS
		(SELECT * FROM {{ source('airbnb_raw', 'listings') }})

	SELECT
		scraped_date,
		listing_id,
		host_id,
		price,
		has_availability,
		availability_30,
		number_of_reviews,
		review_scores_rating,
		review_scores_accuracy,
		review_scores_cleanliness,
		review_scores_checkin,
		review_scores_communication,
		review_scores_value
	FROM listings_source