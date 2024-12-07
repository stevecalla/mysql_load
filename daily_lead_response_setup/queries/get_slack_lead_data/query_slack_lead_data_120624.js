async function query_lead_data() {
    return `
        SELECT
            CAST(created_on_pst AS CHAR) AS created_on_pst

            -- COMMENT OUT THE THREE FIELDS BELOW AS WELL AS THE RELATED GROUP BY TO SEE A ROLLUP OF THE DATA
            , query_source
            , renting_in_country
            , source_name
            , shift
            , response_time_bin

            -- LEAD COUNTS
            , CAST(IFNULL(SUM(count_lead_id), 0) AS UNSIGNED) AS count_leads_total
            , CAST(IFNULL(SUM(CASE WHEN lead_status_id IN (16) THEN count_lead_id END), 0) AS UNSIGNED) AS count_leads_invalid
            , CAST(IFNULL(SUM(CASE WHEN lead_status_id NOT IN (16) THEN count_lead_id END), 0) AS UNSIGNED) AS count_leads_valid
            
            -- SAME DAY COUNTS
            , COUNT(DISTINCT CASE 
                WHEN 
                    DATE_FORMAT(CONVERT_TZ(booking_created_on_utc, '+00:00', '+05:00'), '%Y-%m-%d') = DATE_FORMAT(created_on_pst, '%Y-%m-%d')
                    AND rental_status IN (8)
                    AND lead_status_id NOT IN (16)
                    AND TIMESTAMPDIFF(DAY, created_on_pst, CONVERT_TZ(booking_created_on_utc, '+00:00', '+05:00')) <= 7
                THEN booking_id 
            END) AS count_booking_same_day_rental_status_cancelled_distinct
            
            , COUNT(DISTINCT CASE 
                WHEN 
                    DATE_FORMAT(CONVERT_TZ(booking_created_on_utc, '+00:00', '+05:00'), '%Y-%m-%d') = DATE_FORMAT(created_on_pst, '%Y-%m-%d')
                    AND rental_status NOT IN (8)
                    AND lead_status_id NOT IN (16)
                    AND TIMESTAMPDIFF(DAY, created_on_pst, CONVERT_TZ(booking_created_on_utc, '+00:00', '+05:00')) <= 7
                THEN booking_id 
            END) AS count_booking_same_day_rental_status_not_cancelled_distinct
            
            , COUNT(DISTINCT CASE 
                WHEN 
                    DATE_FORMAT(CONVERT_TZ(booking_created_on_utc, '+00:00', '+05:00'), '%Y-%m-%d') = DATE_FORMAT(created_on_pst, '%Y-%m-%d')
                    -- AND rental_status NOT IN (8)
                    AND lead_status_id NOT IN (16)
                    AND TIMESTAMPDIFF(DAY, created_on_pst, CONVERT_TZ(booking_created_on_utc, '+00:00', '+05:00')) <= 7
                THEN booking_id 
            END) AS count_booking_same_day_rental_status_distinct_total
            
            -- TOTAL COUNTS
            , COUNT(CASE 
                WHEN 
                    rental_status IN (8)
                    AND lead_status_id NOT IN (16)
                    AND TIMESTAMPDIFF(DAY, created_on_pst, CONVERT_TZ(booking_created_on_utc, '+00:00', '+05:00')) <= 7
                THEN booking_id 
            END) AS count_booking_id_cancelled_total
            
            , COUNT(CASE 
                WHEN 
                    rental_status NOT IN (8)
                    AND lead_status_id NOT IN (16)
                    AND TIMESTAMPDIFF(DAY, created_on_pst, CONVERT_TZ(booking_created_on_utc, '+00:00', '+05:00')) <= 7
                THEN booking_id 
            END) AS count_booking_id_not_cancelled_total
            
            -- , COUNT(booking_id) AS count_booking_id_total
            , COUNT(CASE 
                WHEN
                    booking_id IS NOT NULL
                    AND lead_status_id NOT IN (16)
                    AND TIMESTAMPDIFF(DAY, created_on_pst, CONVERT_TZ(booking_created_on_utc, '+00:00', '+05:00')) <= 7 THEN booking_id 
            END) AS count_booking_id_total

            -- CURRENT DATE / TIME PST (Pakistan Standard Time)
            , DATE_FORMAT(UTC_TIMESTAMP(), '%Y-%m-%d %H:%i:%s') AS queried_at_utc
            , DATE_FORMAT(DATE_ADD(UTC_TIMESTAMP(), INTERVAL 4 HOUR), '%Y-%m-%d %H:%i:%s') AS queried_at_gst -- UTC to GST
            , DATE_FORMAT(DATE_ADD(UTC_TIMESTAMP(), INTERVAL 5 HOUR), '%Y-%m-%d %H:%i:%s') AS queried_at_pst -- UTC to PST (Pakistan Standard Time)

            -- Max created_on for all records
            , (	
                SELECT 
                    CAST(MAX(max_created_on_gst) AS CHAR)
                FROM lead_response_data
                LIMIT 1
            ) AS max_created_on_gst

        FROM lead_response_data
        GROUP BY created_on_pst, 2, 3, 4, 5, 6
        -- GROUP BY created_on_pst
        ORDER BY created_on_pst DESC
    `;
}

module.exports = {
    query_lead_data,
}