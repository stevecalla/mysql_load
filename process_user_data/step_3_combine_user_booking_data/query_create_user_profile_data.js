const query_create_user_profile_data = `
-- STEP #3: CREATE USER PROFILE DATA
    CREATE TABLE user_data_profile AS
        SELECT 
            ubd.user_ptr_id,

            ubd.first_name,
            ubd.last_name,
            ubd.email,

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

            -- REPEAT vs NEW USER
            CASE
                WHEN ubd.repeated_user = "YES" THEN 'Yes'
                WHEN ubd.repeated_user = "NO" THEN 'No'
                ELSE ''
            END AS is_repeat_user,
            
            -- REPEAT, NEW VS FIRST
            udkm.is_repeat_new_first,

            -- BOOKING COUNT STATS
            udkm.booking_count_total,
            udkm.booking_count_cancel,
            udkm.booking_count_completed,
            udkm.booking_count_started,
            udkm.booking_count_future,
            udkm.booking_count_other,

            -- -- REVENUE STATS
            udkm.booking_charge_total_less_discount_aed,
            udkm.booking_charge_total_less_discount_extension_aed,
            udkm.booking_charge_extension_only_aed,

            -- -- DAYS STATS
            udkm.booking_days_total,
            udkm.booking_days_initial_only,
            udkm.booking_days_extension_only,

            -- KEY DATE METRICS
            -- MOST RECENT DATES
            udkm.booking_first_created_date,
            udkm.booking_most_recent_created_on,
            udkm.booking_most_recent_pickup_date,
            udkm.booking_most_recent_return_date,
            
            -- DATE COMPARISONS
            udkm.booking_join_vs_first_created, 
            udkm.booking_most_recent_created_on_vs_now,
            udkm.booking_most_recent_return_vs_now,

            -- UTC NOW CONVERTED TO GST
            udkm.date_now_gst

        FROM ezhire_user_data.user_data_combined_booking_data AS ubd
            LEFT JOIN user_data_key_metrics_rollup AS udkm ON udkm.user_ptr_id = ubd.user_ptr_id
            -- LEFT JOIN user_data_combined_booking_data AS ubdv2 ON ubdv2.user_ptr_id = ubd.user_ptr_id
        -- WHERE 
        --     ubd.date_join_formatted_gst = '2024-01-01'
            -- AND
            -- ubd.user_ptr_id IN ('549331')
        GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39
        ORDER BY ubd.user_ptr_id;
`;

module.exports = { query_create_user_profile_data };