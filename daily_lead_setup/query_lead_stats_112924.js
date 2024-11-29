function query_lead_stats() {
    return `
        -- c:\Users\calla\development\ezhire\mysql_queries\leads\discovery_booking_master_112424.sql
        -- #6) FINAL ROLLUP FOR SLACK PROGRAM
        SELECT
            DATE_FORMAT(lm.created_on, '%Y-%m-%d') AS created_on_pst,
            lm.renting_in_country,
            ls.source_name,

            -- LEAD COUNTS
            COUNT(lm.lead_id) AS count_leads_total,
            COUNT(CASE WHEN lm.lead_status_id IN (16) THEN lm.lead_id END) AS count_leads_invalid,
            COUNT(CASE WHEN lm.lead_status_id NOT IN (16) THEN lm.lead_id END) AS count_leads_valid,

            -- Count distinct booking IDs where booking_created_on matches lm.created_on
            COUNT(DISTINCT CASE 
                WHEN 
                    DATE_FORMAT(CONVERT_TZ(bm.booking_created_on, '+00:00', '+05:00'), '%Y-%m-%d') = DATE_FORMAT(lm.created_on, '%Y-%m-%d')
                    AND bm.rental_status IN (8)
                    AND lm.lead_status_id NOT IN (16)
                    AND TIMESTAMPDIFF(DAY, lm.created_on, CONVERT_TZ(bm.booking_created_on, '+00:00', '+05:00')) <= 7
                THEN bm.Booking_id 
            END) AS count_booking_same_day_rental_status_cancelled_distinct, 

            -- Count distinct booking IDs where booking_created_on matches lm.created_on
            COUNT(DISTINCT CASE 
                WHEN 
                    DATE_FORMAT(CONVERT_TZ(bm.booking_created_on, '+00:00', '+05:00'), '%Y-%m-%d') = DATE_FORMAT(lm.created_on, '%Y-%m-%d')
                    AND bm.rental_status NOT IN (8)
                    AND lm.lead_status_id NOT IN (16)
                    AND TIMESTAMPDIFF(DAY, lm.created_on, CONVERT_TZ(bm.booking_created_on, '+00:00', '+05:00')) <= 7
                THEN bm.Booking_id 
            END) AS count_booking_same_day_not_cancelled_distinct,

            -- Count booking IDs where booking_created_on matches lm.created_on
            COUNT(CASE 
                WHEN 
                    bm.rental_status IN (8)
                    AND lm.lead_status_id NOT IN (16)
                    AND TIMESTAMPDIFF(DAY, lm.created_on, CONVERT_TZ(bm.booking_created_on, '+00:00', '+05:00')) <= 7
                THEN bm.Booking_id 
            END) AS count_booking_id_cancelled_total,

            -- Count booking IDs where booking_created_on matches lm.created_on
            COUNT(CASE 
                WHEN 
                    bm.rental_status NOT IN (8)
                    AND lm.lead_status_id NOT IN (16)
                    AND TIMESTAMPDIFF(DAY, lm.created_on, CONVERT_TZ(bm.booking_created_on, '+00:00', '+05:00')) <= 7
                THEN bm.Booking_id 
            END) AS count_booking_id_not_cancelled_total,

            -- Count total booking records
            COUNT(bm.Booking_id) AS count_booking_id_total,
                    
            -- CURRENT DATE / TIME PST (Pakistan Standard Time)
            DATE_FORMAT(NOW(), '%Y-%m-%d %H:%i:%s') AS queried_at_utc,
            DATE_FORMAT(DATE_ADD(NOW(), INTERVAL 4 HOUR), '%Y-%m-%d %H:%i:%s') AS queried_at_gst, -- UTC to PST (Pakistan Standard Time)
            DATE_FORMAT(DATE_ADD(NOW(), INTERVAL 5 HOUR), '%Y-%m-%d %H:%i:%s') AS queried_at_pst, -- UTC to PST (Pakistan Standard Time)

            -- Max created_on for all records (without per-grouping)
            (
                SELECT 
                    -- DATE_FORMAT(MAX(created_on), '%Y-%m-%d %H:%i:%s') -- pst
                    DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 1 HOUR), '%Y-%m-%d %H:%i:%s') AS queried_at_gst -- PST (Pakistan Standard Time) to GST
                FROM leads_master 
                LIMIT 1
            ) AS max_created_on_gst

        FROM leads_master AS lm
            LEFT JOIN booking_master AS bm ON lm.app_booking_id = bm.Booking_id
            LEFT JOIN lead_status AS st ON lm.lead_status_id = st.id
            LEFT JOIN lead_sources AS ls ON lm.lead_source_id = ls.id
        WHERE 
            -- To remove marketing promo leads
            (
                lm.lead_status_id NOT IN (12, 13, 14) OR 
                (
                    COALESCE(bm.promo_code, '') NOT IN (SELECT promo_code FROM conversion_excluded_promo_codes WHERE is_active = 1) 
                    OR COALESCE(bm.promo_code, '') = '' 
                    OR TIMESTAMPDIFF(DAY, lm.created_on, CONVERT_TZ(bm.booking_created_on, '+00:00', '+05:00')) <= 7
                )
            )
            -- LIMIT DATA TO LAST TWO DAYS
            AND lm.created_on >= DATE_FORMAT(DATE_ADD(NOW(), INTERVAL 5 HOUR), '%Y-%m-%d') - INTERVAL 1 DAY -- UTC to PST (Pakistan Standard Time)
        GROUP BY DATE_FORMAT(lm.created_on, '%Y-%m-%d'), 2, 3
        ORDER BY DATE_FORMAT(lm.created_on, '%Y-%m-%d') DESC
        ;
    -- **************************
    `;
}

module.exports = {
    query_lead_stats,
}