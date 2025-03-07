function query_insert_rfm_data() {
	return `INSERT INTO rfm_score_summary_history_data (
		user_ptr_id,date_join_cohort,email,mobile,telephone,first_name,last_name,
		
		all_countries_distinct,
		all_cities_distinct,
				
		all_promo_codes_distinct,
		promo_code_on_most_recent_booking,
		used_promo_code_last_14_days_flag,
		used_promo_code_on_every_booking,

		booking_type_all_distinct, 
		booking_type_most_recent,

		booking_count_total,booking_count_cancel,booking_count_completed,booking_count_started,booking_count_future,booking_count_other,is_currently_started,is_repeat_new_first,is_renter,is_looker,is_canceller,booking_most_recent_return_date,booking_most_recent_return_vs_now,recency_rank,row_number_id,total_rows,row_percent,recency_score_three_parts,recency_score_five_parts,created_at,total_days_per_completed_and_started_bookings,booking_charge__less_discount_aed_per_completed_started_bookings,score_three_parts,three_parts_first_recency_amount,three_parts_last_recency_amount,three_parts_first_frequency_amount,three_parts_last_frequency_amount,three_parts_first_monetary_amount,three_parts_last_monetary_amount,score_five_parts,five_parts_first_recency_amount,five_parts_last_recency_amount,five_parts_first_frequency_amount,five_parts_last_frequency_amount,five_parts_first_monetary_amount,five_parts_last_monetary_amount,test_group,created_at_date)
		
		SELECT 
			*,
			DATE_FORMAT(created_at, '%Y-%m-%d') AS created_at_date
		FROM rfm_score_summary_data;
	`
}

module.exports = { 
	query_insert_rfm_data
};