function query_create_rfm_ranking(table, metric, metric_as) {
	return `
		CREATE TABLE ${table}
			SELECT 
				user_ptr_id,
				is_currently_started,
				is_renter,
				is_looker,
				is_canceller,
				booking_most_recent_return_date,
				rfm_${metric}_metric AS ${metric_as},
		
				FORMAT(percent_rank() OVER (ORDER BY rfm_${metric}_metric), 2) AS ${metric}_rank,
				ROW_NUMBER() OVER (ORDER BY rfm_${metric}_metric, booking_most_recent_return_date) AS row_number_id,
				COUNT(*) OVER () AS total_rows,
				ROW_NUMBER() OVER (ORDER BY rfm_${metric}_metric, booking_most_recent_return_date) / COUNT(*) OVER () AS row_percent,
		
				NTILE(3) OVER (ORDER BY rfm_${metric}_metric ASC) AS ${metric}_score_three_parts,
				-- CASE
				-- 	WHEN ROW_NUMBER() OVER (ORDER BY rfm_${metric}_metric, booking_most_recent_return_date) / COUNT(*) OVER () < 0.33 THEN 1
				-- 	WHEN ROW_NUMBER() OVER (ORDER BY rfm_${metric}_metric, booking_most_recent_return_date) / COUNT(*) OVER () < 0.66 THEN 2
				--     ELSE 3
				-- END AS ${metric}_score_three_parts,
		
				NTILE(5) OVER (ORDER BY rfm_${metric}_metric ASC) AS ${metric}_score_five_parts
				-- CASE
				-- 	WHEN ROW_NUMBER() OVER (ORDER BY rfm_${metric}_metric, booking_most_recent_return_date) / COUNT(*) OVER () < 0.20 THEN 1
				-- 	WHEN ROW_NUMBER() OVER (ORDER BY rfm_${metric}_metric, booking_most_recent_return_date) / COUNT(*) OVER () < 0.40 THEN 2
				-- 	WHEN ROW_NUMBER() OVER (ORDER BY rfm_${metric}_metric, booking_most_recent_return_date) / COUNT(*) OVER () < 0.60 THEN 3
				-- 	WHEN ROW_NUMBER() OVER (ORDER BY rfm_${metric}_metric, booking_most_recent_return_date) / COUNT(*) OVER () < 0.80 THEN 4
				--     ELSE 5
				-- END AS ${metric}_score_five_parts, -- scoring dividing the data into three equal parts
				-- CASE
				-- 	WHEN rfm_${metric}_metric BETWEEN 205 AND 208 THEN 3
				--     WHEN rfm_${metric}_metric BETWEEN 209 and 213 THEN 2
				--     ELSE 1
				-- END AS ${metric}_score_custom_parts
		
			FROM user_data_profile
			--  TOTAL WITH NO FILTER = 594,998
			WHERE 
				is_renter = "yes" -- 95,129 row(s) IN ('repeat', 'first', 'new')
					AND is_currently_started LIKE "no" -- 92,200 row(s)
					AND booking_count_future = 0 
					AND booking_count_other = 0 -- 91,989 row(s)
					AND booking_most_recent_return_date IS NOT NULL -- 91,984 row(s)
					AND rfm_recency_metric >= 0 -- 91,950 row(s)
					AND booking_charge__less_discount_aed_per_completed_started_bookings >= 0 -- 91,813 row(s)
					AND total_days_per_completed_and_started_bookings >= 0 -- 91,813
					AND all_countries_distinct LIKE '%United Arab Emirates%' -- 80,487 UAE combined with other countries
					-- AND all_countries_distinct LIKE 'United Arab Emirates' -- 80,105 only UAE (not UAE and other countries)
			ORDER BY rfm_${metric}_metric, booking_most_recent_return_date ASC;
	`
}

module.exports = { 
	query_create_rfm_ranking
};