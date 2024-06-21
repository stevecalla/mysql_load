function query_create_rfm_score_summary_data() {
	return `
	CREATE TABLE rfm_score_summary_data
		SELECT
			r.*,    
			-- r.booking_most_recent_return_vs_now,
			f.total_days_per_completed_and_started_bookings,
			m.booking_charge__less_discount_aed_per_completed_started_bookings,
	
			-- FIRST & LAST VALUE - THREE PARTS
			CONCAT(r.recency_score_three_parts, f.frequency_score_three_parts, m.monetary_score_three_parts) AS score_three_parts,
			
			FIRST_VALUE(r.booking_most_recent_return_vs_now) OVER (PARTITION BY r.recency_score_three_parts ORDER BY r.booking_most_recent_return_vs_now DESC) AS three_parts_first_recency_amount,    
			LAST_VALUE(r.booking_most_recent_return_vs_now) OVER (
				PARTITION BY r.recency_score_three_parts 
				ORDER BY r.booking_most_recent_return_vs_now DESC
				ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
			) AS three_parts_last_recency_amount,
			
			FIRST_VALUE(f.total_days_per_completed_and_started_bookings) OVER (PARTITION BY f.frequency_score_three_parts ORDER BY f.total_days_per_completed_and_started_bookings DESC) AS three_parts_first_frequency_amount,    
			LAST_VALUE(f.total_days_per_completed_and_started_bookings) OVER (
				PARTITION BY f.frequency_score_three_parts 
				ORDER BY f.total_days_per_completed_and_started_bookings DESC
				ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
			) AS three_parts_last_frequency_amount,
			
			FIRST_VALUE(m.booking_charge__less_discount_aed_per_completed_started_bookings) OVER (PARTITION BY m.monetary_score_three_parts ORDER BY m.booking_charge__less_discount_aed_per_completed_started_bookings DESC) AS three_parts_first_monetary_amount,    
			LAST_VALUE(m.booking_charge__less_discount_aed_per_completed_started_bookings) OVER (
				PARTITION BY m.monetary_score_three_parts 
				ORDER BY m.booking_charge__less_discount_aed_per_completed_started_bookings DESC
				ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
			) AS three_parts_last_monetary_amount,
	
			-- FIRST & LAST VALUE - FIVE PARTS
			CONCAT(r.recency_score_five_parts, f.frequency_score_five_parts, m.monetary_score_five_parts) AS score_five_parts,
	
			FIRST_VALUE(r.booking_most_recent_return_vs_now) OVER (PARTITION BY r.recency_score_five_parts ORDER BY r.booking_most_recent_return_vs_now DESC) AS five_parts_first_recency_amount,    
			LAST_VALUE(r.booking_most_recent_return_vs_now) OVER (
				PARTITION BY r.recency_score_five_parts 
				ORDER BY r.booking_most_recent_return_vs_now DESC
				ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
			) AS five_parts_last_recency_amount,
			
			FIRST_VALUE(f.total_days_per_completed_and_started_bookings) OVER (PARTITION BY f.frequency_score_five_parts ORDER BY f.total_days_per_completed_and_started_bookings DESC) AS five_parts_first_frequency_amount,    
			LAST_VALUE(f.total_days_per_completed_and_started_bookings) OVER (
				PARTITION BY f.frequency_score_five_parts 
				ORDER BY f.total_days_per_completed_and_started_bookings DESC
				ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
			) AS five_parts_last_frequency_amount,
			
			FIRST_VALUE(m.booking_charge__less_discount_aed_per_completed_started_bookings) OVER (PARTITION BY m.monetary_score_five_parts ORDER BY m.booking_charge__less_discount_aed_per_completed_started_bookings DESC) AS five_parts_first_monetary_amount,    
			LAST_VALUE(m.booking_charge__less_discount_aed_per_completed_started_bookings) OVER (
				PARTITION BY m.monetary_score_five_parts 
				ORDER BY m.booking_charge__less_discount_aed_per_completed_started_bookings DESC
				ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
			) AS five_parts_last_monetary_amount
	
			-- -- FIRST & LAST VALUE - CUSTOM PARTS
			-- CONCAT(r.recency_score_custom_parts, f.frequency_score_custom_parts, m.monetary_score_custom_parts) AS score_custom_parts,
	
			-- FIRST_VALUE(r.booking_most_recent_return_vs_now) OVER (PARTITION BY r.recency_score_custom_parts ORDER BY r.booking_most_recent_return_vs_now DESC) AS custom_parts_first_recency_amount,    
			-- LAST_VALUE(r.booking_most_recent_return_vs_now) OVER (
			--     PARTITION BY r.recency_score_custom_parts 
			--     ORDER BY r.booking_most_recent_return_vs_now DESC
			--     ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
			-- ) AS custom_parts_last_recency_amount,
			
			-- FIRST_VALUE(f.total_days_per_completed_and_started_bookings) OVER (PARTITION BY f.frequency_score_custom_parts ORDER BY f.total_days_per_completed_and_started_bookings DESC) AS custom_parts_first_frequency_amount,    
			-- LAST_VALUE(f.total_days_per_completed_and_started_bookings) OVER (
			--     PARTITION BY f.frequency_score_custom_parts 
			--     ORDER BY f.total_days_per_completed_and_started_bookings DESC
			--     ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
			-- ) AS custom_parts_last_frequency_amount,
			
			-- FIRST_VALUE(m.booking_charge__less_discount_aed_per_completed_started_bookings) OVER (PARTITION BY m.monetary_score_custom_parts ORDER BY m.booking_charge__less_discount_aed_per_completed_started_bookings DESC) AS custom_parts_first_monetary_amount,    
			-- LAST_VALUE(m.booking_charge__less_discount_aed_per_completed_started_bookings) OVER (
			--     PARTITION BY m.monetary_score_custom_parts 
			--     ORDER BY m.booking_charge__less_discount_aed_per_completed_started_bookings DESC
			--     ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
			-- ) AS custom_parts_last_monetary_amount
	
		FROM rfm_score_recency_data as r
			INNER JOIN rfm_score_frequency_data AS f ON r.user_ptr_id = f.user_ptr_id
			INNER JOIN rfm_score_monetary_data AS m ON r.user_ptr_id = m.user_ptr_id
		ORDER BY r.booking_most_recent_return_date DESC;
	`
}

module.exports = { 
	query_create_rfm_score_summary_data
};