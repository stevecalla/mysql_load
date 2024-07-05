function query_get_min_and_max_created_at_dates() {
	return `
		-- GET MIN & MAX CREATED AT DATE
		SELECT
			MIN(created_at_date) AS min_created_at_date,
			STR_TO_DATE(MIN(created_at_date), '%m/%d/%Y') AS min_created_at_date_formatted,
		MAX(created_at_date) AS max_created_at_date
		FROM rfm_score_summary_history_data
		LIMIT 1;
	`;
};

function query_get_most_recent_min_and_max_created_at_dates() {
	return `
		-- GET THE TWO MOST RECENT CREATED AT DATES
		SELECT MIN(created_at_date) AS min_created_at_date,
			STR_TO_DATE(MIN(created_at_date), '%m/%d/%Y') AS min_created_at_date_formatted,
			MAX(created_at_date) AS max_created_at_date
		FROM (
			SELECT created_at_date
			FROM rfm_score_summary_history_data
			GROUP BY created_at_date
			ORDER BY created_at_date DESC
			LIMIT 2
		) AS recent_dates;
	`;
};

function query_drop_rfm_score_summary_history_data_tracking(table) {
	return `DROP TABLE IF EXISTS ${table};`
};

function query_create_rfm_score_summary_history_data_tracking(table, min_created_at_date, min_created_at_date_formatted, max_created_at_date) {
	return `
		-- SET @min_created_at_date = '${min_created_at_date}';
		-- SET @min_created_at_date_formatted = '${min_created_at_date_formatted}';
		-- SET @max_created_at_date = '${max_created_at_date}';

		-- DROP TABLE IF EXISTS ${table};

		CREATE TABLE ${table} AS
			SELECT 
				*,
				CASE
					WHEN score_three_parts_as_of_initial_date = 0 THEN 'new'
					WHEN booking_count > 0 THEN 'booker'
					WHEN score_three_parts_difference = 0 THEN 'same'
					WHEN score_three_parts_difference <> 0 THEN 'migrated'
					ELSE 'unknown'
				END AS rfm_segment_three_parts,
				CASE
					WHEN score_five_parts_as_of_initial_date = 0 THEN 'new'
					WHEN booking_count > 0 THEN 'booker'
					WHEN score_five_parts_difference = 0 THEN 'same'
					WHEN score_five_parts_difference <> 0 THEN 'migrated'
					ELSE 'unknown'
				END AS rfm_segment_five_parts
			FROM (
				SELECT
					rfm.user_ptr_id,
					rfm.date_join_cohort,
					b.booking_id,
					b.status,
					b.booking_type,
					b.deliver_method,
					b.car_cat_name,
					b.marketplace_or_dispatch,
					b.promo_code,
					b.booking_date,
					b.pickup_date,
					b.return_date,
					b.booking_charge_less_discount,
					
					COUNT(b.booking_id) AS booking_count,
					'${min_created_at_date}' AS min_created_at_date,
					'${max_created_at_date}' AS max_created_at_date,
					
					MIN(CASE WHEN rfm.created_at_date = '${min_created_at_date}' THEN rfm.test_group ELSE NULL END) AS test_group_at_min_created_at_date,
					
					-- SCORE THREE PART COMPARISON
					MAX(CASE WHEN rfm.created_at_date = '${min_created_at_date}' THEN rfm.score_three_parts ELSE 0 END) AS score_three_parts_as_of_initial_date,
					MAX(CASE WHEN rfm.created_at_date = '${max_created_at_date}' THEN rfm.score_three_parts ELSE 0 END) AS score_three_parts_as_of_most_recent_created_at_date,
					MAX(CASE WHEN rfm.created_at_date = '${min_created_at_date}' THEN rfm.score_three_parts ELSE 0 END) - MAX(CASE WHEN rfm.created_at_date = '${max_created_at_date}' THEN rfm.score_three_parts ELSE 0 END) AS score_three_parts_difference,
					
					-- SCORE FIVE PART COMPARISON
					MAX(CASE WHEN rfm.created_at_date = '${min_created_at_date}' THEN rfm.score_five_parts ELSE 0 END) AS score_five_parts_as_of_initial_date,
					MAX(CASE WHEN rfm.created_at_date = '${max_created_at_date}' THEN rfm.score_five_parts ELSE 0 END) AS score_five_parts_as_of_most_recent_created_at_date,
					MAX(CASE WHEN rfm.created_at_date = '${min_created_at_date}' THEN rfm.score_five_parts ELSE 0 END) - MAX(CASE WHEN rfm.created_at_date = '${max_created_at_date}' THEN rfm.score_five_parts ELSE 0 END) AS score_five_parts_difference
				
				FROM rfm_score_summary_history_data AS rfm
				LEFT JOIN user_data_combined_booking_data AS b ON rfm.user_ptr_id = b.user_ptr_id
					AND rfm.created_at_date = '${min_created_at_date}'
					AND b.booking_date >= '${min_created_at_date_formatted}'
					AND b.status NOT IN ('Cancelled by User')
				GROUP BY rfm.user_ptr_id, rfm.date_join_cohort, b.booking_id, b.status, b.booking_type, b.deliver_method, b.car_cat_name, b.marketplace_or_dispatch, b.promo_code, b.booking_date, b.pickup_date, b.return_date, b.booking_charge_less_discount
		) AS a;

	`
};

module.exports = {
	query_get_min_and_max_created_at_dates,
	query_get_most_recent_min_and_max_created_at_dates,
	query_drop_rfm_score_summary_history_data_tracking,
	query_create_rfm_score_summary_history_data_tracking,
};