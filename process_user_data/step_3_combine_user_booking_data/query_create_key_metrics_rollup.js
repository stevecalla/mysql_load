const query_create_key_metrics_rollup = `
    CREATE TABLE user_data_key_metrics_rollup AS
        SELECT 
            user_ptr_id,
            -- booking_id, -- to see the detail for each booking_id,
            
            -- REPEAT, NEW VS FIRST
            CASE
                -- multiple bookings
                WHEN COUNT(booking_id) >= 2 THEN 'Repeat' 
                -- booking within 48 hours of join datetime
                WHEN DATEDIFF(MIN(booking_date), MIN(date_join_formatted_gst)) <= 2 AND COUNT(booking_id) = 1 THEN 'New' 
                -- 1 booking after 48 hours of join datetime
                WHEN DATEDIFF(MIN(booking_date), MIN(date_join_formatted_gst)) > 2 AND COUNT(booking_id) = 1 THEN 'First' 
                ELSE ''
            END AS is_repeat_new_first,

            -- BOOKING COUNT STATS
            COUNT(booking_id) AS booking_count_total,
            SUM(CASE WHEN status LIKE '%cancelled%' THEN 1 ELSE 0 END) AS booking_count_cancel,
            SUM(CASE WHEN status LIKE '%ended%' THEN 1 ELSE 0 END) AS booking_count_completed,
            SUM(CASE WHEN status LIKE '%started%' THEN 1 ELSE 0 END) AS booking_count_started,
            SUM(CASE WHEN status LIKE '%future%' THEN 1 ELSE 0 END) AS booking_count_future,
            SUM(CASE WHEN status NOT LIKE '%cancelled%' AND status NOT LIKE '%ended%' AND status NOT LIKE '%started%' AND status NOT LIKE '%future%' THEN 1 ELSE 0 END) AS booking_count_other,

            -- REVENUE STATS
            SUM(CASE WHEN status NOT LIKE '%cancelled%' THEN booking_charge_less_discount_aed ELSE 0 END) AS booking_charge_total_less_discount_aed, 
            SUM(CASE WHEN status NOT LIKE '%cancelled%' THEN booking_charge_less_discount_extension_aed ELSE 0 END) AS booking_charge_total_less_discount_extension_aed,
            SUM(CASE WHEN status NOT LIKE '%cancelled%' THEN extension_charge_aed ELSE 0 END) AS booking_charge_extension_only_aed,

            -- DAYS STATS
            SUM(CASE WHEN status NOT LIKE '%cancelled%' THEN days ELSE 0 END) AS booking_days_total,
            SUM(CASE WHEN status NOT LIKE '%cancelled%' THEN (days - extension_days) ELSE 0 END) AS booking_days_initial_only,
            SUM(CASE WHEN status NOT LIKE '%cancelled%' THEN extension_days ELSE 0 END) AS booking_days_extension_only,
	
            -- MOST RECENT DATES
            CASE
                WHEN MIN(booking_date) IS NULL THEN NULL
                ELSE CAST(IFNULL(MIN(booking_date), '') AS DATE)
            END AS booking_first_created_date,
            CASE
                WHEN MAX(booking_date) IS NULL THEN NULL
                ELSE CAST(IFNULL(MAX(booking_date), '') AS DATE)
            END AS booking_most_recent_created_on,
            CASE
                WHEN MAX(pickup_date) IS NULL THEN NULL
                ELSE CAST(IFNULL(MAX(pickup_date), '') AS DATE)
            END AS booking_most_recent_pickup_date,
            CASE
                WHEN MAX(return_date) IS NULL THEN NULL
                ELSE CAST(IFNULL(MAX(return_date), '') AS DATE)
            END AS booking_most_recent_return_date,
            
            -- DATE COMPARISONS
            CAST(
                IFNULL(TIMESTAMPDIFF(DAY, MIN(date_join_formatted_gst), MIN(booking_date)), '') 
                AS DOUBLE
            ) AS booking_join_vs_first_created,
            CAST(
                IFNULL(TIMESTAMPDIFF(DAY, MIN(booking_date), MIN(pickup_date)), '') 
                AS DOUBLE
            ) AS booking_first_created_vs_first_pickup,
            CAST(
                IFNULL(TIMESTAMPDIFF(DAY, DATE_FORMAT(MAX(booking_date), '%Y-%m-%d'), DATE_FORMAT(DATE_ADD(UTC_TIMESTAMP(), INTERVAL 4 HOUR), '%Y-%m-%d')), '')
                AS DOUBLE
            ) AS booking_most_recent_created_on_vs_now,
            CAST(
                IFNULL(TIMESTAMPDIFF(DAY, DATE_FORMAT(MAX(return_date), '%Y-%m-%d'), DATE_FORMAT(DATE_ADD(UTC_TIMESTAMP(), INTERVAL 4 HOUR), '%Y-%m-%d')), '')
                AS DOUBLE
            ) AS booking_most_recent_return_vs_now,

            -- UTC NOW CONVERTED TO GST
            DATE_ADD(UTC_TIMESTAMP(), INTERVAL 4 HOUR) AS date_now_gst

        FROM ezhire_user_data.user_data_combined_booking_data
        -- WHERE
        -- 	date_join_formatted_gst >= '2024-01-01'
            -- AND
            -- user_ptr_id IN ('549331')
        -- GROUP BY 1, 2;
        GROUP BY 1;
`;

module.exports = { query_create_key_metrics_rollup };