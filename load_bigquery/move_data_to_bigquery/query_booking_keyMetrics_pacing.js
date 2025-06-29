const bookingQuery = `
    SELECT
        booking_id,
        agreement_number,

        -- DATE_FORMAT(booking_date, '%Y-%m-%d') AS booking_date,
        CASE
            WHEN booking_date IS NULL THEN ''
            ELSE DATE_FORMAT(booking_date, '%Y-%m-%d')
        END AS booking_date,
        -- DATE_FORMAT(CONVERT_TZ(booking_datetime, '+00:00', '-04:00'), '%Y-%m-%d %H:%i:%s UTC') as booking_datetime,
        CASE
            WHEN booking_datetime IS NULL THEN ''
            ELSE DATE_FORMAT(CONVERT_TZ(booking_datetime, '+00:00', '-04:00'), '%Y-%m-%d %H:%i:%s UTC')
        END AS booking_datetime,

        -- max_booking_datetime,
        DATE_FORMAT(CONVERT_TZ(max_booking_datetime, '+00:00', '-04:00'), '%Y-%m-%d %H:%i:%s UTC') as max_booking_datetime,

        today AS is_today,
        
        booking_year,booking_quarter,booking_month,booking_day_of_month,booking_week_of_year,booking_day_of_week,booking_day_of_week_v2,booking_time_bucket,booking_count,booking_count_excluding_cancel,

        -- DATE_FORMAT(pickup_date, '%Y-%m-%d') AS pickup_date,
        CASE
            WHEN pickup_date IS NULL THEN ''
            ELSE DATE_FORMAT(pickup_date, '%Y-%m-%d')
        END AS pickup_date,

        -- DATE_FORMAT(CONVERT_TZ(pickup_datetime, '+00:00', '-04:00'), '%Y-%m-%d %H:%i:%s UTC') as pickup_datetime,
        CASE
            WHEN pickup_datetime IS NULL THEN ''
            ELSE DATE_FORMAT(CONVERT_TZ(pickup_datetime, '+00:00', '-04:00'), '%Y-%m-%d %H:%i:%s UTC')
        END AS pickup_datetime,

        -- NOTE GOOGLE BQ ERRORING WITH NULL VALUE FOR INTEGER; NEEDS TO BE ''
        CASE
            WHEN pickup_year IS NULL THEN ''
            ELSE pickup_year
        END AS pickup_year,
        
        CASE
            WHEN pickup_quarter IS NULL THEN ''
            ELSE pickup_quarter
        END AS pickup_quarter,

        CASE
            WHEN pickup_month IS NULL THEN ''
            ELSE pickup_month
        END AS pickup_month,

        CASE
            WHEN pickup_day_of_month IS NULL THEN ''
            ELSE pickup_day_of_month
        END AS pickup_day_of_month,
        
        CASE
            WHEN pickup_week_of_year IS NULL THEN ''
            ELSE pickup_week_of_year
        END AS pickup_week_of_year,

        CASE
            WHEN pickup_day_of_week IS NULL THEN ''
            ELSE pickup_day_of_week
        END AS pickup_day_of_week,

        pickup_day_of_week_v2,
        CASE
            WHEN pickup_time_bucket IS NULL THEN ''
            ELSE pickup_time_bucket
        END AS pickup_time_bucket,

        CASE
            WHEN return_date IS NULL THEN ''
            ELSE DATE_FORMAT(return_date, '%Y-%m-%d')
        END AS return_date,

        -- DATE_FORMAT(CONVERT_TZ(return_datetime, '+00:00', '-04:00'), '%Y-%m-%d %H:%i:%s UTC') as return_datetime,
        CASE
            WHEN return_datetime IS NULL THEN ''
            ELSE DATE_FORMAT(CONVERT_TZ(return_datetime, '+00:00', '-04:00'), '%Y-%m-%d %H:%i:%s UTC')
        END AS return_datetime,

        -- NOTE GOOGLE BQ ERRORING WITH NULL VALUE FOR INTEGER; NEEDS TO BE ''
        CASE
            WHEN return_year IS NULL THEN ''
            ELSE return_year
        END AS return_year,

        CASE
            WHEN return_quarter IS NULL THEN ''
            ELSE return_quarter
        END AS return_quarter,

        CASE
            WHEN return_month IS NULL THEN ''
            ELSE return_month
        END AS return_month,

        CASE
            WHEN return_day_of_month IS NULL THEN ''
            ELSE return_day_of_month
        END AS return_day_of_month,

        CASE
            WHEN return_week_of_year IS NULL THEN ''
            ELSE return_week_of_year
        END AS return_week_of_year,

        CASE
            WHEN return_day_of_week IS NULL THEN ''
            ELSE return_day_of_week
        END AS return_day_of_week,

        return_day_of_week_v2,
        
        CASE
            WHEN return_time_bucket IS NULL THEN ''
            ELSE return_time_bucket
        END AS return_time_bucket,
        
        advance_category_day,advance_category_week,advance_category_month,advance_category_date_within_week,advance_pickup_booking_date_diff,comparison_28_days,comparison_period,

        -- comparison_common_date,
        CASE
            WHEN comparison_common_date IS NULL THEN ''
            ELSE DATE_FORMAT(comparison_common_date, '%Y-%m-%d')
        END AS comparison_common_date,
        
        Current_28_Days,4_Weeks_Prior,52_Weeks_Prior,status,booking_type,marketplace_or_dispatch,marketplace_partner,marketplace_partner_summary,booking_channel,booking_source,repeated_user,total_lifetime_booking_revenue,no_of_bookings,no_of_cancel_bookings,no_of_completed_bookings,no_of_started_bookings,customer_id,first_name,last_name,email,

        CASE
            WHEN date_of_birth IS NULL THEN ''
            WHEN date_of_birth = '' THEN ''
            WHEN STR_TO_DATE(date_of_birth, '%m/%d/%Y') IS NULL THEN ''
            WHEN STR_TO_DATE(date_of_birth, '%m/%d/%Y') < '1900-01-01' THEN ''
            WHEN STR_TO_DATE(date_of_birth, '%m/%d/%Y') > CURDATE() THEN ''
            ELSE DATE_FORMAT(STR_TO_DATE(date_of_birth, '%m/%d/%Y'), '%Y-%m-%d')
        END AS date_of_birth,

        age,
        
        -- date_join_formatted_gst,
        CASE
            WHEN date_join_formatted_gst IS NULL THEN ''
            ELSE DATE_FORMAT(date_join_formatted_gst, '%Y-%m-%d')
        END AS date_join_formatted_gst,

        date_join_cohort,
        date_join_year,
        date_join_month,
        
        resident_category,
        
        customer_driving_country,customer_doc_vertification_status,days,extension_days,extra_day_calc,customer_rate,insurance_rate,additional_driver_rate,pai_rate,baby_seat_rate,insurance_type,millage_rate,millage_cap_km,rent_charge,rent_charge_less_discount_extension_aed,extra_day_charge,delivery_charge,collection_charge,additional_driver_charge,insurance_charge,pai_charge,baby_charge,long_distance,
        
        premium_delivery,airport_delivery,gps_charge,delivery_update,intercity_charge,millage_charge,other_rental_charge,discount_charge,discount_charge_aed,
        
        discount_extension_charge,total_vat,other_charge,booking_charge,booking_charge_less_discount,booking_charge_aed,booking_charge_less_discount_aed,booking_charge_less_extension,booking_charge_less_discount_extension,booking_charge_less_extension_aed,booking_charge_less_discount_extension_aed,base_rental_revenue,non_rental_charge,extension_charge,extension_charge_aed,is_extended,
        
        promo_code,
        promo_code_discount_amount,
        promocode_created_date, -- currently all null values
        CASE
            WHEN promocode_created_date IS NULL THEN ''
            ELSE promocode_created_date
        END AS promocode_created_date,
        promo_code_description,
        promo_code_department,
        -- promo_code_expiration_date,
        CASE
            WHEN promo_code_expiration_date IS NULL THEN ''
            ELSE DATE_FORMAT(promo_code_expiration_date, '%Y-%m-%d')
        END AS promo_code_expiration_date,
        
        car_avail_id,car_cat_id,car_cat_name,requested_car,car_name,make,color,deliver_country,deliver_city,country_id,city_id,delivery_location,deliver_method,
        
        -- delivery_lat,
        SUBSTRING_INDEX(delivery_lat, ' ', 1) AS delivery_lat,

        delivery_lng,collection_location,collection_method,
        
        -- collection_lat,
        SUBSTRING_INDEX(collection_lat, ' ', 1) AS collection_lat,
        collection_lng,
        
        nps_score,nps_comment,

        DATE_FORMAT(CONVERT_TZ(created_at, '+00:00', '+07:00'), '%Y-%m-%d %H:%i:%s UTC') as created_at
        
    FROM ezhire_booking_data.booking_data 
    WHERE 1 = 1
        AND booking_year >= 2023

    -- WHERE pickup_year IN (2023, 2024, 2025)
    -- WHERE pickup_year IN ('2024')
    -- WHERE booking_date IN ('2024-01-01')
    -- WHERE status NOT LIKE '%Cancel%'
    -- AND pickup_year IN (2023, 2024, 2025)
    -- AND comparison_28_days = "Yes"
    -- AND booking_id IN ('159514', '159877', '197222') -- fix date of birth
    -- AND booking_id IN ('160123') -- fix null values for year, month et al for each date

    ORDER BY booking_date ASC, pickup_date ASC
    -- LIMIT 1;
`;

const keyMetricsQuery = `
    SELECT
        DATE_FORMAT(CONVERT_TZ(created_at, '+00:00', '+07:00'), '%Y-%m-%d %H:%i:%s UTC') as created_at,
        DATE_FORMAT(STR_TO_DATE(calendar_date, '%Y-%m-%d'), '%Y-%m-%d') AS calendar_date,

        year,quarter,month,week,day,
        
        DATE_FORMAT(CONVERT_TZ(max_booking_datetime, '+00:00', '-04:00'), '%Y-%m-%d %H:%i:%s UTC') as max_booking_datetime,
        is_today,
        
        days_on_rent_whole_day,days_on_rent_fraction,trans_on_rent_count,booking_count,pickup_count,return_count,day_in_initial_period,day_in_extension_period,booking_charge_aed_rev_allocation,booking_charge_less_discount_aed_rev_allocation,rev_aed_in_initial_period,rev_aed_in_extension_period,
        
        vendor_on_rent_dispatch,
        vendor_on_rent_marketplace,
        booking_type_on_rent_daily,
        booking_type_on_rent_monthly,
        booking_type_on_rent_subscription,
        booking_type_on_rent_weekly,
        is_repeat_on_rent_no,
        is_repeat_on_rent_yes,
        country_on_rent_bahrain,
        -- country_on_rent_georgia,
        -- country_on_rent_kuwait,
        -- country_on_rent_oman,
        -- country_on_rent_pakistan,
        country_on_rent_qatar,
        country_on_rent_saudia_arabia,
        -- country_on_rent_serbia,
        country_on_rent_united_arab_emirates
        
    FROM ezhire_key_metrics.key_metrics_data
    ORDER BY calendar_date ASC
    -- LIMIT 1;
`;

const pacingQuery = `
    SELECT
        DATE_FORMAT(CONVERT_TZ(max_booking_datetime, '+00:00', '-04:00'), '%Y-%m-%d %H:%i:%s UTC') as max_booking_datetime,
        is_before_today,

        pickup_month_year,
        DATE_FORMAT(booking_date, '%Y-%m-%d') AS booking_date,

        days_from_first_day_of_month,
        count,
        total_booking_charge_aed,
        total_booking_charge_less_discount_aed,
        total_booking_charge_less_discount_extension_aed,
        total_extension_charge_aed,
        running_count,running_total_booking_charge_aed,
        running_total_booking_charge_less_discount_aed,
        running_total_booking_charge_less_discount_extension_aed,
        running_total_extension_charge_aed,

        DATE_FORMAT(CONVERT_TZ(created_at, '+00:00', '+07:00'), '%Y-%m-%d %H:%i:%s UTC') as created_at

    FROM ezhire_pacing_metrics.pacing_final_data
    ORDER BY pickup_month_year ASC, booking_date ASC
    -- LIMIT 1;
`;

const profileQuery = `
    SELECT
        user_ptr_id, first_name, last_name, email, mobile, telephone,

        DATE_FORMAT(date_of_birth, '%Y-%m-%d') AS date_of_birth,

        age,

        DATE_FORMAT(date_join_formatted_gst, '%Y-%m-%d') AS date_join_formatted_gst,

        date_join_cohort, 
        date_join_year, 
        
        date_join_quarter, date_join_month, date_join_week_of_year, date_join_day_of_year,

        last_login_gst, 
        has_last_login_date, 
        
        -- is_resident, 
        CASE
            WHEN is_resident IN ('? undefined:undefined ?') THEN ''
            ELSE is_resident
        END AS is_resident,

        -- all_resident_category,
        CONCAT('"', all_resident_category, '"') AS all_resident_category,

        most_recent_resident_category,
        
        user_is_verified, 

        -- all_nps_scores,
        -- CONCAT('"', all_nps_scores, '"') AS all_nps_scores,
        CONCAT('"', REPLACE(all_nps_scores, ',', '|'), '"') AS all_nps_scores,

        most_recent_nps_score,
		most_recent_nps_comment,

        -- all_booking_ids,
        CONCAT('"', all_booking_ids, '"') AS all_booking_ids,

        booking_count_extended,
        
        is_repeat_user, 
        is_repeat_new_first,
         
        booking_count_total, booking_count_cancel, booking_count_completed, booking_count_started,
        booking_count_future, booking_count_other, booking_count_not_cancel, 
        
        -- wrap fields in double quotes to avoid issues with comma parsing in CSV files
        CONCAT('"', all_countries_distinct, '"') AS all_countries_distinct,
        CONCAT('"', all_cities_distinct, '"') AS all_cities_distinct,  
        
        -- wrap fields in double quotes to avoid issues with comma parsing in CSV files
        CONCAT('"', all_promo_codes_distinct, '"') AS all_promo_codes_distinct,
        CONCAT('"', promo_code_on_most_recent_booking, '"') AS promo_code_on_most_recent_booking,
        used_promo_code_last_14_days_flag,
		used_promo_code_on_every_booking,
        
        -- wrap fields in double quotes to avoid issues with comma parsing in CSV files
        CONCAT('"', booking_type_all_distinct, '"') AS booking_type_all_distinct,
        CONCAT('"', booking_type_most_recent, '"') AS booking_type_most_recent,

        -- booking_charge_total_less_discount_aed, 
        CAST(ROUND(booking_charge_total_less_discount_aed, 2) AS DECIMAL(10,2)) AS booking_charge_total_less_discount_aed,

        -- booking_charge_total_less_discount_extension_aed,
        CAST(ROUND(booking_charge_total_less_discount_extension_aed, 2) AS DECIMAL(10,2)) AS booking_charge_total_less_discount_extension_aed,

        -- booking_charge_extension_only_aed, 
        CAST(ROUND(booking_charge_extension_only_aed, 2) AS DECIMAL(10,2)) AS booking_charge_extension_only_aed,
        
        -- booking_days_total, 
        CAST(ROUND(booking_days_total, 2) AS DECIMAL(10,2)) AS booking_days_total,

        -- booking_days_initial_only, 
        CAST(ROUND(booking_days_initial_only, 2) AS DECIMAL(10,2)) AS booking_days_initial_only,

        -- booking_days_extension_only, 
        CAST(ROUND(booking_days_extension_only, 2) AS DECIMAL(10,2)) AS booking_days_extension_only,
        
        DATE_FORMAT(booking_first_created_date, '%Y-%m-%d') AS booking_first_created_date,
        DATE_FORMAT(booking_most_recent_created_date, '%Y-%m-%d') AS booking_most_recent_created_date,
        DATE_FORMAT(booking_most_recent_pickup_date, '%Y-%m-%d') AS booking_most_recent_pickup_date,
        DATE_FORMAT(booking_most_recent_return_date, '%Y-%m-%d') AS booking_most_recent_return_date,
        
        booking_join_vs_first_created, booking_first_created_vs_first_pickup, booking_most_recent_created_on_vs_now, 
        booking_most_recent_return_vs_now, 
        total_days_per_completed_and_started_bookings, booking_charge__less_discount_aed_per_completed_started_bookings, 

        DATE_FORMAT(CONVERT_TZ(date_now_gst, '+00:00', '+00:00'), '%Y-%m-%d %H:%i:%s Asia/Dubai') as date_now_gst_v2,
        
        is_currently_started, is_canceller, is_renter, is_looker, is_other,
        rfm_recency_metric, rfm_frequency_metric, rfm_monetary_metric,

        DATE_FORMAT(CONVERT_TZ(created_at, '+00:00', '+07:00'), '%Y-%m-%d %H:%i:%s UTC') as created_at

    FROM ezhire_user_data.user_data_profile
    -- original testing example
    -- WHERE user_ptr_id = 40
    -- for 161 needed to fix the distinct city / country to wrap with quotes to escape the comma
    -- WHERE user_ptr_id = 161
    -- LIMIT 500000;
`;

const cohortQuery = `
    SELECT 
        DATE_FORMAT(calendar_date, '%Y-%m-%d') AS calendar_date,
        
        calendar_year_month,calendar_year,calendar_quarter,calendar_month,calendar_week,
        calendar_day,
        
        DATE_FORMAT(CONVERT_TZ(max_booking_datetime, '+00:00', '-04:00'), '%Y-%m-%d %H:%i:%s UTC') as max_booking_datetime,
        
        cohort_join_year_month,cohort_join_year,cohort_join_quarter,cohort_join_month,
        cohort_month_start,calendar_month_start,months_diff,is_today,days_on_rent_whole_day,days_on_rent_fraction,
        trans_on_rent_count,booking_count,pickup_count,return_count,day_in_initial_period,day_in_extension_period,
        booking_charge_aed_rev_allocation,booking_charge_less_discount_aed_rev_allocation,rev_aed_in_initial_period,
        rev_aed_in_extension_period,vendor_on_rent_dispatch,vendor_on_rent_marketplace,booking_type_on_rent_daily,
        booking_type_on_rent_monthly,booking_type_on_rent_subscription,booking_type_on_rent_weekly,is_repeat_on_rent_no,
        is_repeat_on_rent_yes,country_on_rent_bahrain,country_on_rent_georgia,country_on_rent_kuwait,
        country_on_rent_oman,country_on_rent_pakistan,country_on_rent_qatar,country_on_rent_saudia_arabia,
        country_on_rent_serbia,country_on_rent_united_arab_emirates,
        
        DATE_FORMAT(CONVERT_TZ(created_at, '+00:00', '+07:00'), '%Y-%m-%d %H:%i:%s UTC') as created_at

    FROM ezhire_user_data.user_data_cohort_stats 
    -- LIMIT 200000;
`;

const rfmQuery = `
    SELECT 
        user_ptr_id,date_join_cohort,email,mobile,telephone,

        first_name,
        last_name,
        
        -- wrap fields in double quotes to avoid issues with comma parsing in CSV files
        CONCAT('"', all_countries_distinct, '"') AS all_countries_distinct,
        CONCAT('"', all_cities_distinct, '"') AS all_cities_distinct,
        
        -- wrap fields in double quotes to avoid issues with comma parsing in CSV files
        CONCAT('"', all_promo_codes_distinct, '"') AS all_promo_codes_distinct,
        CONCAT('"', promo_code_on_most_recent_booking, '"') AS promo_code_on_most_recent_booking,
        used_promo_code_last_14_days_flag,
		used_promo_code_on_every_booking,
        
        -- wrap fields in double quotes to avoid issues with comma parsing in CSV files
        CONCAT('"', booking_type_all_distinct, '"') AS booking_type_all_distinct,
        CONCAT('"', booking_type_most_recent, '"') AS booking_type_most_recent,
        
        booking_count_total,booking_count_cancel,
        booking_count_completed,booking_count_started,booking_count_future,booking_count_other,is_currently_started,
        is_repeat_new_first,is_renter,is_looker,is_canceller,
        
        DATE_FORMAT(booking_most_recent_return_date, '%Y-%m-%d') AS booking_most_recent_return_date,

        booking_most_recent_return_vs_now,recency_rank,row_number_id,total_rows,row_percent,recency_score_three_parts,
        recency_score_five_parts,
    
        total_days_per_completed_and_started_bookings,
        booking_charge__less_discount_aed_per_completed_started_bookings,score_three_parts,three_parts_first_recency_amount,
        three_parts_last_recency_amount,three_parts_first_frequency_amount,three_parts_last_frequency_amount,
        three_parts_first_monetary_amount,three_parts_last_monetary_amount,score_five_parts,five_parts_first_recency_amount,
        five_parts_last_recency_amount,five_parts_first_frequency_amount,five_parts_last_frequency_amount,
        five_parts_first_monetary_amount,five_parts_last_monetary_amount,
        
        test_group,

        DATE_FORMAT(CONVERT_TZ(created_at, '+00:00', '+07:00'), '%Y-%m-%d %H:%i:%s UTC') as created_at
    
    FROM ezhire_user_data.rfm_score_summary_data 
    -- LIMIT 200000;
`;

const rfmTrackingQuery = `
    SELECT
        user_ptr_id,date_join_cohort,
        
		is_repeat_new_first,

        -- wrap fields in double quotes to avoid issues with comma parsing in CSV files
        CONCAT('"', all_countries_distinct, '"') AS all_countries_distinct,
        CONCAT('"', all_cities_distinct, '"') AS all_cities_distinct,

        -- wrap fields in double quotes to avoid issues with comma parsing in CSV files
        CONCAT('"', all_promo_codes_distinct, '"') AS all_promo_codes_distinct,
        CONCAT('"', promo_code_on_most_recent_booking, '"') AS promo_code_on_most_recent_booking,
        used_promo_code_last_14_days_flag,
		used_promo_code_on_every_booking,
        
        -- wrap fields in double quotes to avoid issues with comma parsing in CSV files
        CONCAT('"', booking_type_all_distinct, '"') AS booking_type_all_distinct,
        CONCAT('"', booking_type_most_recent, '"') AS booking_type_most_recent,

		booking_count_total,
		booking_count_cancel,
		booking_count_completed,
		booking_count_started,
		booking_count_future,
		booking_count_other,
		is_currently_started,
        
        booking_id,status,booking_type,deliver_method,car_cat_name,marketplace_or_dispatch,
        
        promo_code,has_promo_code,
        
        CASE
            WHEN booking_date IS NULL THEN ''
            ELSE DATE_FORMAT(booking_date, '%Y-%m-%d')
        END AS booking_date,

        CASE
            WHEN pickup_date IS NULL THEN ''
            ELSE DATE_FORMAT(pickup_date, '%Y-%m-%d')
        END AS pickup_date,

        CASE
            WHEN return_date IS NULL THEN ''
            ELSE DATE_FORMAT(return_date, '%Y-%m-%d')
        END AS return_date,

        days,booking_charge_less_discount,
    
		-- RFM TEST GROUPS
        test_group_at_min_created_at_date,
    
		-- RFM SCORE METRICS
        booking_most_recent_return_vs_now,
        total_days_per_completed_and_started_bookings,
        booking_charge_less_discount_aed_per_completed_started_bookings,

        -- SCORE THREE PART COMPARISON
        IFNULL(score_three_parts_as_of_initial_date, '') AS score_three_parts_as_of_initial_date,
        IFNULL(score_three_parts_as_of_most_recent_created_at_date, '') AS score_three_parts_as_of_most_recent_created_at_date,
        IFNULL(score_three_parts_difference, '') AS score_three_parts_difference,

        -- SCORE FIVE PART COMPARISON
        IFNULL(score_five_parts_as_of_initial_date, '') AS score_five_parts_as_of_initial_date,
        IFNULL(score_five_parts_as_of_most_recent_created_at_date, '') AS score_five_parts_as_of_most_recent_created_at_date,
        IFNULL(score_five_parts_difference, '') AS score_five_parts_difference,
        
        booking_count,
        
        CASE
            WHEN min_created_at_date IS NULL THEN ''
            ELSE DATE_FORMAT(min_created_at_date, '%Y-%m-%d')
        END AS min_created_at_date,
        CASE
            WHEN max_created_at_date IS NULL THEN ''
            ELSE DATE_FORMAT(max_created_at_date, '%Y-%m-%d')
        END AS max_created_at_date,
        
        -- SCORE SEGMENTS
        rfm_segment_three_parts,rfm_segment_five_parts,
        
        DATE_FORMAT(CONVERT_TZ(created_at, '+00:00', '+07:00'), '%Y-%m-%d %H:%i:%s UTC') as created_at

    FROM ezhire_user_data.rfm_score_summary_history_data_tracking
`;

const rfmTrackingMostRecentQuery = `
    SELECT
        user_ptr_id,date_join_cohort,
        
		is_repeat_new_first,

        -- wrap fields in double quotes to avoid issues with comma parsing in CSV files
        CONCAT('"', all_countries_distinct, '"') AS all_countries_distinct,
        CONCAT('"', all_cities_distinct, '"') AS all_cities_distinct,

        -- wrap fields in double quotes to avoid issues with comma parsing in CSV files
        CONCAT('"', all_promo_codes_distinct, '"') AS all_promo_codes_distinct,
        CONCAT('"', promo_code_on_most_recent_booking, '"') AS promo_code_on_most_recent_booking,
        used_promo_code_last_14_days_flag,
		used_promo_code_on_every_booking,
        
        -- wrap fields in double quotes to avoid issues with comma parsing in CSV files
        CONCAT('"', booking_type_all_distinct, '"') AS booking_type_all_distinct,
        CONCAT('"', booking_type_most_recent, '"') AS booking_type_most_recent,

		booking_count_total,
		booking_count_cancel,
		booking_count_completed,
		booking_count_started,
		booking_count_future,
		booking_count_other,
		is_currently_started,
        
        booking_id,status,booking_type,deliver_method,car_cat_name,marketplace_or_dispatch,
        
        promo_code,has_promo_code,
        
        CASE
            WHEN booking_date IS NULL THEN ''
            ELSE DATE_FORMAT(booking_date, '%Y-%m-%d')
        END AS booking_date,

        CASE
            WHEN pickup_date IS NULL THEN ''
            ELSE DATE_FORMAT(pickup_date, '%Y-%m-%d')
        END AS pickup_date,

        CASE
            WHEN return_date IS NULL THEN ''
            ELSE DATE_FORMAT(return_date, '%Y-%m-%d')
        END AS return_date,

        days,booking_charge_less_discount,
    
		-- RFM TEST GROUPS
        test_group_at_min_created_at_date,
    
		-- RFM SCORE METRICS
        booking_most_recent_return_vs_now,
        total_days_per_completed_and_started_bookings,
        booking_charge_less_discount_aed_per_completed_started_bookings,

        -- SCORE THREE PART COMPARISON
        IFNULL(score_three_parts_as_of_initial_date, '') AS score_three_parts_as_of_initial_date,
        IFNULL(score_three_parts_as_of_most_recent_created_at_date, '') AS score_three_parts_as_of_most_recent_created_at_date,
        IFNULL(score_three_parts_difference, '') AS score_three_parts_difference,

        -- SCORE FIVE PART COMPARISON
        IFNULL(score_five_parts_as_of_initial_date, '') AS score_five_parts_as_of_initial_date,
        IFNULL(score_five_parts_as_of_most_recent_created_at_date, '') AS score_five_parts_as_of_most_recent_created_at_date,
        IFNULL(score_five_parts_difference, '') AS score_five_parts_difference,

        booking_count,
    
        CASE
            WHEN booking_date IS NULL THEN ''
            ELSE DATE_FORMAT(min_created_at_date, '%Y-%m-%d')
        END AS min_created_at_date,
        CASE
            WHEN booking_date IS NULL THEN ''
            ELSE DATE_FORMAT(max_created_at_date, '%Y-%m-%d')
        END AS max_created_at_date,
        
        -- SCORE SEGMENTS
        rfm_segment_three_parts,rfm_segment_five_parts,
        
        DATE_FORMAT(CONVERT_TZ(created_at, '+00:00', '+07:00'), '%Y-%m-%d %H:%i:%s UTC') as created_at

    FROM ezhire_user_data.rfm_score_summary_history_data_tracking_most_recent
`;

const rfmTrackingOffersQuery = `
    SELECT
        user_ptr_id,date_join_cohort,
        
		is_repeat_new_first,

        -- wrap fields in double quotes to avoid issues with comma parsing in CSV files
        CONCAT('"', all_countries_distinct, '"') AS all_countries_distinct,
        CONCAT('"', all_cities_distinct, '"') AS all_cities_distinct,

        -- wrap fields in double quotes to avoid issues with comma parsing in CSV files
        CONCAT('"', all_promo_codes_distinct, '"') AS all_promo_codes_distinct,
        CONCAT('"', promo_code_on_most_recent_booking, '"') AS promo_code_on_most_recent_booking,
        used_promo_code_last_14_days_flag,
		used_promo_code_on_every_booking,
        
        -- wrap fields in double quotes to avoid issues with comma parsing in CSV files
        CONCAT('"', booking_type_all_distinct, '"') AS booking_type_all_distinct,
        CONCAT('"', booking_type_most_recent, '"') AS booking_type_most_recent,

		booking_count_total,
		booking_count_cancel,
		booking_count_completed,
		booking_count_started,
		booking_count_future,
		booking_count_other,
		is_currently_started,
        
        booking_id,status,booking_type,deliver_method,car_cat_name,marketplace_or_dispatch,
        
        promo_code,has_promo_code,
        
        CASE
            WHEN booking_date IS NULL THEN ''
            ELSE DATE_FORMAT(booking_date, '%Y-%m-%d')
        END AS booking_date,

        CASE
            WHEN pickup_date IS NULL THEN ''
            ELSE DATE_FORMAT(pickup_date, '%Y-%m-%d')
        END AS pickup_date,

        CASE
            WHEN return_date IS NULL THEN ''
            ELSE DATE_FORMAT(return_date, '%Y-%m-%d')
        END AS return_date,

        days,booking_charge_less_discount,
    
		-- RFM TEST GROUPS
        test_group_at_min_created_at_date,
    
		-- RFM SCORE METRICS
        booking_most_recent_return_vs_now,
        total_days_per_completed_and_started_bookings,
        booking_charge_less_discount_aed_per_completed_started_bookings,

        -- SCORE THREE PART COMPARISON
        IFNULL(score_three_parts_as_of_initial_date, '') AS score_three_parts_as_of_initial_date,
        IFNULL(score_three_parts_as_of_most_recent_created_at_date, '') AS score_three_parts_as_of_most_recent_created_at_date,
        IFNULL(score_three_parts_difference, '') AS score_three_parts_difference,

        -- SCORE FIVE PART COMPARISON
        IFNULL(score_five_parts_as_of_initial_date, '') AS score_five_parts_as_of_initial_date,
        IFNULL(score_five_parts_as_of_most_recent_created_at_date, '') AS score_five_parts_as_of_most_recent_created_at_date,
        IFNULL(score_five_parts_difference, '') AS score_five_parts_difference,
        
        booking_count,
        
        CASE
            WHEN min_created_at_date IS NULL THEN ''
            ELSE DATE_FORMAT(min_created_at_date, '%Y-%m-%d')
        END AS min_created_at_date,
        CASE
            WHEN max_created_at_date IS NULL THEN ''
            ELSE DATE_FORMAT(max_created_at_date, '%Y-%m-%d')
        END AS max_created_at_date,
        
        -- SCORE SEGMENTS
        rfm_segment_three_parts,rfm_segment_five_parts,
        
        DATE_FORMAT(CONVERT_TZ(created_at, '+00:00', '+07:00'), '%Y-%m-%d %H:%i:%s UTC') as created_at

    FROM ezhire_user_data.rfm_score_summary_history_data_tracking_offer
`;

const rfmTrackingOffersV2Query = `
    SELECT
        user_ptr_id,date_join_cohort,
        
		is_repeat_new_first,

        -- wrap fields in double quotes to avoid issues with comma parsing in CSV files
        CONCAT('"', all_countries_distinct, '"') AS all_countries_distinct,
        CONCAT('"', all_cities_distinct, '"') AS all_cities_distinct,

        -- wrap fields in double quotes to avoid issues with comma parsing in CSV files
        CONCAT('"', all_promo_codes_distinct, '"') AS all_promo_codes_distinct,
        CONCAT('"', promo_code_on_most_recent_booking, '"') AS promo_code_on_most_recent_booking,
        used_promo_code_last_14_days_flag,
		used_promo_code_on_every_booking,
        
        -- wrap fields in double quotes to avoid issues with comma parsing in CSV files
        CONCAT('"', booking_type_all_distinct, '"') AS booking_type_all_distinct,
        CONCAT('"', booking_type_most_recent, '"') AS booking_type_most_recent,

		booking_count_total,
		booking_count_cancel,
		booking_count_completed,
		booking_count_started,
		booking_count_future,
		booking_count_other,
		is_currently_started,
        
        booking_id,status,booking_type,deliver_method,car_cat_name,marketplace_or_dispatch,
        
        promo_code,has_promo_code,
        
        CASE
            WHEN booking_date IS NULL THEN ''
            ELSE DATE_FORMAT(booking_date, '%Y-%m-%d')
        END AS booking_date,

        CASE
            WHEN pickup_date IS NULL THEN ''
            ELSE DATE_FORMAT(pickup_date, '%Y-%m-%d')
        END AS pickup_date,

        CASE
            WHEN return_date IS NULL THEN ''
            ELSE DATE_FORMAT(return_date, '%Y-%m-%d')
        END AS return_date,

        days,booking_charge_less_discount,
    
		-- RFM TEST GROUPS
        test_group_at_min_created_at_date,
    
		-- RFM SCORE METRICS
        booking_most_recent_return_vs_now,
        total_days_per_completed_and_started_bookings,
        booking_charge_less_discount_aed_per_completed_started_bookings,

        -- SCORE THREE PART COMPARISON
        IFNULL(score_three_parts_as_of_initial_date, '') AS score_three_parts_as_of_initial_date,
        IFNULL(score_three_parts_as_of_most_recent_created_at_date, '') AS score_three_parts_as_of_most_recent_created_at_date,
        IFNULL(score_three_parts_difference, '') AS score_three_parts_difference,

        -- SCORE FIVE PART COMPARISON
        IFNULL(score_five_parts_as_of_initial_date, '') AS score_five_parts_as_of_initial_date,
        IFNULL(score_five_parts_as_of_most_recent_created_at_date, '') AS score_five_parts_as_of_most_recent_created_at_date,
        IFNULL(score_five_parts_difference, '') AS score_five_parts_difference,
        
        booking_count,
        
        CASE
            WHEN min_created_at_date IS NULL THEN ''
            ELSE DATE_FORMAT(min_created_at_date, '%Y-%m-%d')
        END AS min_created_at_date,
        CASE
            WHEN max_created_at_date IS NULL THEN ''
            ELSE DATE_FORMAT(max_created_at_date, '%Y-%m-%d')
        END AS max_created_at_date,
        
        -- SCORE SEGMENTS
        rfm_segment_three_parts,rfm_segment_five_parts,
        
        DATE_FORMAT(CONVERT_TZ(created_at, '+00:00', '+07:00'), '%Y-%m-%d %H:%i:%s UTC') as created_at

    FROM ezhire_user_data.rfm_score_summary_history_data_tracking_offer_v2
`;

const rfmTrackingOffersV3Query = `
    SELECT
        user_ptr_id,date_join_cohort,
        
		is_repeat_new_first,

        -- wrap fields in double quotes to avoid issues with comma parsing in CSV files
        CONCAT('"', all_countries_distinct, '"') AS all_countries_distinct,
        CONCAT('"', all_cities_distinct, '"') AS all_cities_distinct,

        -- wrap fields in double quotes to avoid issues with comma parsing in CSV files
        CONCAT('"', all_promo_codes_distinct, '"') AS all_promo_codes_distinct,
        CONCAT('"', promo_code_on_most_recent_booking, '"') AS promo_code_on_most_recent_booking,
        used_promo_code_last_14_days_flag,
		used_promo_code_on_every_booking,
        
        -- wrap fields in double quotes to avoid issues with comma parsing in CSV files
        CONCAT('"', booking_type_all_distinct, '"') AS booking_type_all_distinct,
        CONCAT('"', booking_type_most_recent, '"') AS booking_type_most_recent,

		booking_count_total,
		booking_count_cancel,
		booking_count_completed,
		booking_count_started,
		booking_count_future,
		booking_count_other,
		is_currently_started,
        
        booking_id,status,booking_type,deliver_method,car_cat_name,marketplace_or_dispatch,
        
        promo_code,has_promo_code,
        
        CASE
            WHEN booking_date IS NULL THEN ''
            ELSE DATE_FORMAT(booking_date, '%Y-%m-%d')
        END AS booking_date,

        CASE
            WHEN pickup_date IS NULL THEN ''
            ELSE DATE_FORMAT(pickup_date, '%Y-%m-%d')
        END AS pickup_date,

        CASE
            WHEN return_date IS NULL THEN ''
            ELSE DATE_FORMAT(return_date, '%Y-%m-%d')
        END AS return_date,

        days,booking_charge_less_discount,
    
		-- RFM TEST GROUPS
        test_group_at_min_created_at_date,
    
		-- RFM SCORE METRICS
        booking_most_recent_return_vs_now,
        total_days_per_completed_and_started_bookings,
        booking_charge_less_discount_aed_per_completed_started_bookings,

        -- SCORE THREE PART COMPARISON
        IFNULL(score_three_parts_as_of_initial_date, '') AS score_three_parts_as_of_initial_date,
        IFNULL(score_three_parts_as_of_most_recent_created_at_date, '') AS score_three_parts_as_of_most_recent_created_at_date,
        IFNULL(score_three_parts_difference, '') AS score_three_parts_difference,

        -- SCORE FIVE PART COMPARISON
        IFNULL(score_five_parts_as_of_initial_date, '') AS score_five_parts_as_of_initial_date,
        IFNULL(score_five_parts_as_of_most_recent_created_at_date, '') AS score_five_parts_as_of_most_recent_created_at_date,
        IFNULL(score_five_parts_difference, '') AS score_five_parts_difference,
        
        booking_count,
        
        CASE
            WHEN min_created_at_date IS NULL THEN ''
            ELSE DATE_FORMAT(min_created_at_date, '%Y-%m-%d')
        END AS min_created_at_date,
        CASE
            WHEN max_created_at_date IS NULL THEN ''
            ELSE DATE_FORMAT(max_created_at_date, '%Y-%m-%d')
        END AS max_created_at_date,
        
        -- SCORE SEGMENTS
        rfm_segment_three_parts,rfm_segment_five_parts,
        
        DATE_FORMAT(CONVERT_TZ(created_at, '+00:00', '+07:00'), '%Y-%m-%d %H:%i:%s UTC') as created_at

    FROM ezhire_user_data.rfm_score_summary_history_data_tracking_offer_v3
`;

module.exports = {
    bookingQuery,
    keyMetricsQuery,
    pacingQuery,
    profileQuery,
    cohortQuery,
    rfmQuery,
    rfmTrackingQuery,
    rfmTrackingMostRecentQuery,
    rfmTrackingOffersQuery,
    rfmTrackingOffersV2Query,
    rfmTrackingOffersV3Query,
}

//NOTE: Need to fix date formats so biqquery would convert properly; BQ saves all timestamps in UTC thus need to convert for analysis/Looker queries 
//NOTE: source: https://www.googlecloudcommunity.com/gc/Data-Analytics/Timezone-in-BigQuery/m-p/698624
//NOTE: source: https://en.wikipedia.org/wiki/List_of_tz_database_time_zones

// DATE_FORMAT(booking_date, '%Y-%m-%d') AS booking_date
// converts 2022-11-08T07:00:00.000Z to '2022-11-08'

// DATE_FORMAT(CONVERT_TZ(booking_datetime, '+00:00', '+00:00'), '%Y-%m-%d %H:%i:%s Asia/Dubai') as booking_datetime,
    // converts 2024-01-01T07:18:36.000Z to '2024-01-01 00:18:36 Asia/Dubai'
    // then biquery will convert GST to UTC (less 4 hours)

// DATE_FORMAT(created_at, '%Y-%m-%d %H:%i:%s MST') as created_at
    // converts timestamp 2024-04-03T20:38:20.000Z to the format 2018-07-05 12:54:00 MST
    // then biquery will convert MST to UTC (or +7 hours)

// DATE_FORMAT(STR_TO_DATE('2023-01-01', '%Y-%m-%d'), '%Y-%m-%d') AS calendar_date,
    // converts string '2023-01-01' to date

// DATE_FORMAT(STR_TO_DATE(date_of_birth, '%m/%d/%Y'), '%Y-%m-%d') AS date_of_birth
    // converts string '01/1/1995' to '1995-01-01' 


    