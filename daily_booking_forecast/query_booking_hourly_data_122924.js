function query_booking_hourly_data() {
    return `
        -- C:\Users\calla\development\ezhire\mysql_queries\daily_booking_forecast\forecast_draft_122724.sql

        SET @today_timestamp_utc = UTC_TIMESTAMP();
        SET @today_date_gst = DATE_FORMAT(DATE_ADD(UTC_TIMESTAMP(), INTERVAL 4 HOUR), '%Y-%m-%d');
        SET @today_timestamp_gst = DATE_ADD(UTC_TIMESTAMP(), INTERVAL 4 HOUR);
        SET @today_current_hour_gst = DATE_FORMAT(DATE_ADD(UTC_TIMESTAMP(), INTERVAL 4 HOUR), '%H');
        SET @today_current_dayofweek_gst = DAYOFWEEK(DATE_ADD(UTC_TIMESTAMP(), INTERVAL 4 HOUR));
        SET @same_day_last_week = DATE_FORMAT(DATE_SUB(@today_date_gst, INTERVAL 7 DAY), '%Y-%m-%d');

        WITH actual_last_7_days AS (
                SELECT
                    DATE_FORMAT(DATE_ADD(b.created_on, INTERVAL 4 HOUR), '%H') AS booking_time_bucket,
                    DATE_FORMAT(DATE_ADD(b.created_on, INTERVAL 4 HOUR), '%Y-%m-%d') AS booking_date,
                    "actual" AS segment_major,
                    "actual_last_7_days" AS segment_minor,
                    COUNT(*) AS hourly_bookings
                FROM rental_car_booking2 AS b
                    LEFT JOIN rental_status AS rs ON b.status = rs.id
                    INNER JOIN myproject.rental_city rc ON rc.id = b.city_id
                    INNER JOIN myproject.rental_country co ON co.id = rc.CountryID
                WHERE 
                DATE_FORMAT(DATE_ADD(b.created_on, INTERVAL 4 HOUR), '%Y-%m-%d') BETWEEN 
                    DATE_FORMAT(DATE_SUB(@today_date_gst, INTERVAL 7 DAY), '%Y-%m-%d') AND @today_date_gst
                AND rs.status != "Cancelled by User"
                AND co.name IN ('United Arab Emirates')
                GROUP BY booking_date, booking_time_bucket
                ORDER BY booking_date, booking_time_bucket
        )
        , actual_today AS (
            SELECT
                DATE_FORMAT(DATE_ADD(b.created_on, INTERVAL 4 HOUR), '%H') AS booking_time_bucket,
                DATE_FORMAT(DATE_ADD(b.created_on, INTERVAL 4 HOUR), '%Y-%m-%d') AS booking_date,
                "actual" AS segment_major,
                "actual_today" AS segment_minor,
                COUNT(*) AS hourly_bookings
            FROM rental_car_booking2 AS b
            LEFT JOIN rental_status AS rs ON b.status = rs.id
            INNER JOIN myproject.rental_city rc ON rc.id = b.city_id
            INNER JOIN myproject.rental_country co ON co.id = rc.CountryID
            WHERE 
                DATE_FORMAT(DATE_ADD(b.created_on, INTERVAL 4 HOUR), '%Y-%m-%d') = @today_date_gst
                AND rs.status != "Cancelled by User"
                AND co.name IN ('United Arab Emirates')
            GROUP BY booking_time_bucket
            ORDER BY booking_time_bucket
        )
            -- SELECT * FROM actual_today;
        , actual_7_days_ago AS ( -- same day last week
            SELECT
                DATE_FORMAT(DATE_ADD(b.created_on, INTERVAL 4 HOUR), '%H') AS booking_time_bucket,
                DATE_FORMAT(DATE_ADD(b.created_on, INTERVAL 4 HOUR), '%Y-%m-%d') AS booking_date,
                "actual" AS segment_major,
                "actual_7_days_ago" AS segment_minor,
                COUNT(*) AS hourly_bookings
            FROM rental_car_booking2 AS b
            LEFT JOIN rental_status AS rs ON b.status = rs.id
            INNER JOIN myproject.rental_city rc ON rc.id = b.city_id
            INNER JOIN myproject.rental_country co ON co.id = rc.CountryID
            WHERE 
                DATE_FORMAT(DATE_ADD(b.created_on, INTERVAL 4 HOUR), '%Y-%m-%d') = @same_day_last_week
                AND rs.status != "Cancelled by User"
                AND co.name IN ('United Arab Emirates')
            GROUP BY booking_time_bucket
            ORDER BY booking_time_bucket
        )
            -- SELECT * FROM actual_7_days_ago;
        , actuals_same_day_last_4_weeks AS (
            SELECT
                DATE_FORMAT(DATE_ADD(b.created_on, INTERVAL 4 HOUR), '%H') AS booking_time_bucket,
                DATE_FORMAT(DATE_ADD(b.created_on, INTERVAL 4 HOUR), '%Y-%m-%d') AS booking_date,
                "actual" AS segment_major,
                "actuals_same_day_last_4_weeks" AS segment_minor,
                COUNT(*) AS hourly_bookings
            FROM rental_car_booking2 AS b
                LEFT JOIN rental_status AS rs ON b.status = rs.id
                INNER JOIN myproject.rental_city rc ON rc.id = b.city_id
                INNER JOIN myproject.rental_country co ON co.id = rc.CountryID
            WHERE 
                DATE_FORMAT(DATE_ADD(b.created_on, INTERVAL 4 HOUR), '%Y-%m-%d') BETWEEN 
                    DATE_FORMAT(DATE_SUB(@today_date_gst, INTERVAL 28 DAY), '%Y-%m-%d') AND DATE_SUB(@today_date_gst, INTERVAL 7 DAY)
                AND rs.status != "Cancelled by User"
                AND co.name IN ('United Arab Emirates')
                AND DAYOFWEEK(DATE_ADD(b.created_on, INTERVAL 4 HOUR)) = DAYOFWEEK(@today_date_gst) -- Filter for the same day of the week
            GROUP BY booking_date, booking_time_bucket
            ORDER BY booking_date, booking_time_bucket
        )
            -- SELECT * FROM actuals_same_day_last_4_weeks;
        , average_last_7_days AS (
            SELECT
                booking_time_bucket,
                NULL AS booking_date,
                "average" AS segment_major,
                "average_last_7_days" AS segment_minor,
                ROUND(AVG(hourly_bookings), 0) AS hourly_bookings
            FROM actual_last_7_days
            GROUP BY booking_time_bucket
            ORDER BY booking_time_bucket
        )
            -- SELECT * FROM average_last_7_days;
        , average_same_day_last_4_weeks AS (
            SELECT
                booking_time_bucket,
                NULL AS booking_date,
                "average" AS segment_major,
                "average_same_day_last_4_weeks" AS segment_minor,
                ROUND(AVG(hourly_bookings), 0) AS hourly_bookings
            FROM actuals_same_day_last_4_weeks
            GROUP BY booking_time_bucket
            ORDER BY booking_time_bucket
        )
            -- SELECT * FROM average_same_day_last_4_weeks;
        , estimate_last_7_days AS (
            SELECT
                l7.booking_time_bucket,
                NULL AS booking_date,
                "estimate" AS segment_major,
                "estimate_last_7_days" AS segment_minor,
                CASE
                    WHEN l7.booking_time_bucket < @today_current_hour_gst THEN ac_today.hourly_bookings
                    ELSE ROUND(AVG(l7.hourly_bookings))
                END AS hourly_bookings -- estimate_last_7_days
            FROM average_last_7_days AS l7
                LEFT JOIN actual_today AS ac_today ON l7.booking_time_bucket = ac_today.booking_time_bucket
            GROUP BY l7.booking_time_bucket
            ORDER BY l7.booking_time_bucket
        )
            -- SELECT * FROM actuals_same_day_last_4_weeks;
        , estimate_same_day_last_4_weeks AS (
            SELECT
                l4.booking_time_bucket,
                NULL AS booking_date,
                "estimate" AS segment_major,
                "estimate_same_day_last_4_weeks" AS segment_minor,
                CASE
                    WHEN l4.booking_time_bucket < @today_current_hour_gst THEN ac_today.hourly_bookings
                    ELSE ROUND(AVG(l4.hourly_bookings))
                END AS hourly_bookings -- estimate_same_day_last_4_weeks
            FROM average_same_day_last_4_weeks AS l4
                LEFT JOIN actual_today AS ac_today ON l4.booking_time_bucket = ac_today.booking_time_bucket
            GROUP BY l4.booking_time_bucket
            ORDER BY l4.booking_time_bucket
        )
            -- SELECT * FROM actuals_same_day_last_4_weeks;
        , estimate_same_day_7_days_ago AS (
            SELECT
                ac7.booking_time_bucket,
                NULL AS booking_date,
                "estimate" AS segment_major,
                "estimate_same_day_7_days_ago" AS segment_minor,
                CASE
                    WHEN ac7.booking_time_bucket < @today_current_hour_gst THEN ac_today.hourly_bookings
                    ELSE ROUND(AVG(ac7.hourly_bookings))
                END AS hourly_bookings -- estimate_same_day_7_days_ago
            FROM actual_7_days_ago AS ac7
                LEFT JOIN actual_today AS ac_today ON ac7.booking_time_bucket = ac_today.booking_time_bucket
            GROUP BY ac7.booking_time_bucket
            ORDER BY ac7.booking_time_bucket
        )
            -- SELECT * FROM estimate_same_day_7_days_ago;
        , union_all_data AS (
            SELECT 
                *
            FROM (
                SELECT 
                    booking_date,
                    booking_time_bucket,
                    segment_major,
                    segment_minor, 
                    hourly_bookings
                FROM actual_last_7_days
                
                UNION ALL

                SELECT 
                    booking_date,
                    booking_time_bucket, 
                    segment_major,
                    segment_minor, 
                    hourly_bookings
                FROM actual_today

                UNION ALL

                SELECT 
                    booking_date,
                    booking_time_bucket, 
                    segment_major,
                    segment_minor, 
                    hourly_bookings
                FROM actual_7_days_ago

                UNION ALL 

                SELECT 
                    booking_date,
                    booking_time_bucket, 
                    segment_major,
                    segment_minor, 
                    hourly_bookings
                FROM actuals_same_day_last_4_weeks

                UNION ALL 

                SELECT 
                    booking_date, 
                    booking_time_bucket,
                    segment_major,
                    segment_minor, 
                    hourly_bookings
                FROM average_last_7_days

                UNION ALL

                SELECT 
                    booking_date,
                    booking_time_bucket, 
                    segment_major,
                    segment_minor, 
                    hourly_bookings
                FROM average_same_day_last_4_weeks

                UNION ALL 

                SELECT 
                    booking_date, 
                    booking_time_bucket,
                    segment_major,
                    segment_minor, 
                    hourly_bookings
                FROM estimate_last_7_days

                UNION ALL 

                SELECT 
                    booking_date, 
                    booking_time_bucket,
                    segment_major,
                    segment_minor, 
                    hourly_bookings
                FROM estimate_same_day_last_4_weeks

                UNION ALL 

                SELECT 
                    booking_date, 
                    booking_time_bucket,
                    segment_major,
                    segment_minor, 
                    hourly_bookings
                FROM estimate_same_day_7_days_ago

            ) combined_results
            ORDER BY booking_date ASC, segment_minor, booking_time_bucket
            -- ORDER BY booking_time_bucket ASC, booking_date ASC, segment_minor;
        )
        , final_data_table AS (
            SELECT 
                un.*, 
                DAYOFWEEK(un.booking_date) AS booking_date_day_of_week,
                @today_date_gst AS today_date_gst,
                @today_timestamp_utc AS today_timestamp_ut,
                @today_timestamp_gst AS today_timestamp_gst,
                @today_current_hour_gst AS today_current_hour_gst,
                IF(booking_time_bucket < @today_current_hour_gst, "yes", "no") AS booking_time_bucket_flag,
                @today_current_dayofweek_gst AS today_current_day_of_week_gst,
                @same_day_last_week AS same_day_last_week
            FROM union_all_data AS un
        )
        SELECT * FROM final_data_table;
    `;
}

module.exports = {
    query_booking_hourly_data,
}