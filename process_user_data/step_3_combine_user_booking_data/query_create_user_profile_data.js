const query_create_user_profile_data = `
-- STEP #3: CREATE USER PROFILE DATA
CREATE TABLE user_data_profile AS
	SELECT 
		ubd.user_ptr_id,

    	-- Basic User Details (Assuming 1 record per user)
		ubd.first_name,
		ubd.last_name,
		ubd.email,
		ubd.mobile,
		ubd.telephone,

		-- DATE OF BIRTH / AGE
		ubd.date_of_birth,
		TIMESTAMPDIFF(YEAR, ubd.date_of_birth, CURDATE()) AS age,

		-- JOINED COHORT
		ubd.date_join_formatted_gst,
		ubd.date_join_cohort,
		ubd.date_join_year,
		QUARTER(ubd.date_join_formatted_gst) AS date_join_quarter,
		ubd.date_join_month,
		WEEK(ubd.date_join_formatted_gst) AS date_join_week_of_year,
		DAY(ubd.date_join_formatted_gst) AS date_join_day_of_year,

		-- LAST LOGIN
		IFNULL(DATE_FORMAT(ubd.last_login_gst, '%Y-%m-%d'), '') AS last_login_gst,
		IF(ubd.last_login_gst IS NOT NULL, 'Yes', 'No') AS has_last_login_date,

		-- IS_RESIDENT
		ubd.is_resident,

		-- IS_VERIFIED (DOCUMENTS)
		IFNULL(CASE WHEN ubd.is_verified > 0 THEN 'Yes' ELSE 'No' END, 0) AS user_is_verified,

	-- REPEAT vs NEW USER (using max b/c some user ids have both yes & no)
		MAX(
			CASE 
				WHEN ubd.repeated_user = 'Yes' THEN 'Yes'
				WHEN ubd.repeated_user IS NULL THEN ''
				ELSE 'No'
			END
		) AS is_repeat_user,

		
		-- REPEAT, NEW VS FIRST
		udkm.is_repeat_new_first,

		-- BOOKING COUNT STATS
		udkm.booking_count_total,
		udkm.booking_count_cancel,
		udkm.booking_count_completed,
		udkm.booking_count_started,
		udkm.booking_count_future,
		udkm.booking_count_other,
		udkm.booking_count_not_cancel,

		-- COUNTRY / CITY
		udkm.all_countries_distinct,
		udkm.all_cities_distinct,

		-- PROMO CODE STATUS
		all_promo_codes_distinct,
		promo_code_on_most_recent_booking,
		used_promo_code_last_14_days_flag,
		used_promo_code_on_every_booking,

		-- BOOKING TYPE
		udkm.booking_type_all_distinct,
		udkm.booking_type_most_recent,

		-- REVENUE STATS
		udkm.booking_charge_total_less_discount_aed,
		udkm.booking_charge_total_less_discount_extension_aed,
		udkm.booking_charge_extension_only_aed,

		-- DAYS STATS
		udkm.booking_days_total,
		udkm.booking_days_initial_only,
		udkm.booking_days_extension_only,

		-- KEY DATE METRICS --
		-- MOST RECENT DATES
		udkm.booking_first_created_date,
		udkm.booking_most_recent_created_date,
		udkm.booking_most_recent_pickup_date,
		udkm.booking_most_recent_return_date,
		
		-- DATE COMPARISONS
		udkm.booking_join_vs_first_created,
		udkm.booking_first_created_vs_first_pickup,
		udkm.booking_most_recent_created_on_vs_now,
		udkm.booking_most_recent_return_vs_now, -- recency metric
			
		-- KEY STATS - REVENUE PER BOOKING & DAYS PER BOOKING
		CASE
			WHEN (udkm.booking_count_completed + udkm.booking_count_started) = 0 THEN 0 -- AVOID DIVIDING BY 0
			WHEN udkm.booking_charge_total_less_discount_aed = 0 THEN 0 -- AVOID DIVIDING INTO 0
			ELSE (udkm.booking_days_total) / (udkm.booking_count_completed + udkm.booking_count_started) -- avg days per booking
		END total_days_per_completed_and_started_bookings, -- frequency metric
		CASE
			WHEN (udkm.booking_count_completed + udkm.booking_count_started) = 0 THEN 0 -- AVOID DIVIDING BY 0
			WHEN udkm.booking_charge_total_less_discount_aed = 0 THEN 0 -- AVOID DIVIDING INTO 0
			ELSE (udkm.booking_charge_total_less_discount_aed) / (udkm.booking_count_completed + udkm.booking_count_started) -- revenue per booking
		END AS booking_charge__less_discount_aed_per_completed_started_bookings, -- monetary value metric

		-- UTC NOW CONVERTED TO GST
		udkm.date_now_gst,
		
		-- RFM SCORING --
		-- CURRENT STATUS FOR STARTED & RENTER
		CASE WHEN booking_count_started = 1 THEN "yes" ELSE "no" END AS is_currently_started,
		CASE WHEN is_repeat_new_first IN ('canceller') THEN "yes" ELSE "no" END AS is_canceller,
		CASE WHEN is_repeat_new_first IN ('repeat', 'first', 'new') THEN "yes" ELSE "no" END AS is_renter,
		CASE WHEN is_repeat_new_first IN ('looker') THEN "yes" ELSE "no" END AS is_looker,
		CASE WHEN is_repeat_new_first IN ('other') THEN "yes" ELSE "no" END AS is_other,

		-- RECENCY = DIFF BETWEEN NOW & MOST RECENT RETURN DATE; LOWER NUMBER BETTER
		-- udkm.booking_most_recent_return_vs_now AS rfm_recency_metric,
		CASE
			WHEN udkm.booking_most_recent_return_date IS NULL THEN NULL
			ELSE udkm.booking_most_recent_return_vs_now
		END AS rfm_recency_metric,

		-- FREQUENCY = AVERAGE DAYS PER COMPLETED/STARTED BOOKING
		CASE
			WHEN udkm.booking_most_recent_return_date IS NULL THEN NULL
			WHEN (udkm.booking_count_completed + udkm.booking_count_started) = 0 THEN 0 -- AVOID DIVIDING BY 0
			-- WHEN udkm.booking_charge_total_less_discount_aed = 0 THEN 0 -- AVOID DIVIDING INTO 0
			WHEN udkm.booking_charge_total_less_discount_aed = 0 THEN udkm.booking_days_total -- AVOID DIVIDING INTO 0
			ELSE (udkm.booking_days_total) / (udkm.booking_count_completed + udkm.booking_count_started) -- avg days per booking
		END AS rfm_frequency_metric,

		-- MONETARY VALUE = AVERAGE VALUE PER COMPLETED/STARTED BOOKING
		CASE
			WHEN udkm.booking_most_recent_return_date IS NULL THEN NULL
			WHEN (udkm.booking_count_completed + udkm.booking_count_started) = 0 THEN 0 -- AVOID DIVIDING BY 0
			-- WHEN udkm.booking_charge_total_less_discount_aed = 0 THEN 0 -- AVOID DIVIDING INTO 0
			WHEN udkm.booking_charge_total_less_discount_aed = 0 THEN udkm.booking_charge_total_less_discount_aed -- AVOID DIVIDING INTO 0
			ELSE (udkm.booking_charge_total_less_discount_aed) / (udkm.booking_count_completed + udkm.booking_count_started) -- revenue per booking
		END rfm_monetary_metric

	FROM ezhire_user_data.user_data_combined_booking_data AS ubd
		LEFT JOIN user_data_key_metrics_rollup AS udkm ON udkm.user_ptr_id = ubd.user_ptr_id
	-- WHERE 
		-- ubd.date_join_formatted_gst = '2024-01-01'
		-- AND
		-- ubd.user_ptr_id IN ('711774', '711609', '679185', '471934') -- multiple records due to ubd.repeated_user having both yes & no for some users
		-- ubd.user_ptr_id IN ('711774')
		-- ubd.user_ptr_id IN ('549331')
		-- ubd.user_ptr_id IN ('549331', '419418', '593518', '593396') -- all above first, repeat, canceller, canceller
		-- ubd.user_ptr_id IN ('11022')			-- recency score -196586, most recent return date '2562-08-10' & rental cancelled
		-- booking_count_started = 0
		-- AND
		-- udkm.booking_most_recent_return_vs_now < 0

	-- excluding 20 due to max
	GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53
	ORDER BY ubd.user_ptr_id
`;

module.exports = { query_create_user_profile_data };