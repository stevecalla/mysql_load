function query_lead_stats() {

    const date_interval = 1; // for javascript sql script; also change the variable to ${date_interval}

    return `
        -- #5) QUERY TO FIND THE (a) BOOKINGS WITH MULTIPLE LEADS (b) BOOKINGS WITH 1 LEAD (c) LEADS WITH NO BOOKINGS; COMBINE THE DATA THEN CALCULATE STATS
        -- C:\Users\calla\development\ezhire\mysql_queries\leads\discovery_leads_final_attempt_112924.sql

        -- const date_interval = 1; -- for javascript sql script; also change @date_interval to {date_interval}
        -- modify "=" to ">=" or similar

        SELECT
            created_on_pst

            -- COMMENT OUT THE THREE FIELDS BELOW AS WELL AS THE RELATED GROUP BY TO SEE A ROLLUP OF THE DATA
            , query_source
            , renting_in_country
            , source_name

            -- LEAD COUNTS
            , CAST(SUM(count_lead_id) AS UNSIGNED) AS count_leads_total
            , CAST(SUM(CASE WHEN lead_status_id IN (16) THEN count_lead_id END) AS UNSIGNED) AS count_leads_invalid
            , CAST(SUM(CASE WHEN lead_status_id NOT IN (16) THEN count_lead_id END) AS UNSIGNED) AS count_leads_valid

            
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
            , DATE_FORMAT(NOW(), '%Y-%m-%d %H:%i:%s') AS queried_at_utc
            , DATE_FORMAT(DATE_ADD(NOW(), INTERVAL 4 HOUR), '%Y-%m-%d %H:%i:%s') AS queried_at_gst -- UTC to GST
            , DATE_FORMAT(DATE_ADD(NOW(), INTERVAL 5 HOUR), '%Y-%m-%d %H:%i:%s') AS queried_at_pst -- UTC to PST (Pakistan Standard Time)

            -- Max created_on for all records (without per-grouping)
            , (	
                SELECT 
                    DATE_SUB(DATE_FORMAT(MAX(created_on), '%Y-%m-%d %H:%i:%s'), INTERVAL 1 HOUR)
                FROM leads_master 
                LIMIT 1
            ) AS max_created_on_gst

        -- The subquery combines data from multiple sources and scenarios (multiple leads, single lead, or no booking)
        FROM (
            -- First subquery: Handles cases where multiple leads are associated with a single booking
            SELECT
                DATE_FORMAT(lm.created_on, '%Y-%m-%d') AS created_on_pst,
                bm.Booking_id AS booking_id,

                SUBSTRING_INDEX(GROUP_CONCAT(DISTINCT IF(bm.rental_status IS NULL OR bm.rental_status = '', NULL, bm.rental_status)), ',', 1) AS rental_status,
                SUBSTRING_INDEX(GROUP_CONCAT(DISTINCT IF(lm.lead_status_id IS NULL OR lm.lead_status_id = '', NULL, lm.lead_status_id)), ',', 1) AS lead_status_id,

                -- GROUP_CONCAT(DISTINCT IF(lm.lead_id IS NULL OR lm.lead_id = '', NULL, lm.lead_id)) AS lead_id_list,
                SUBSTRING_INDEX(GROUP_CONCAT(DISTINCT IF(lm.lead_id IS NULL OR lm.lead_id = '', NULL, lm.lead_id)), ',', 1) AS lead_id,
            
                -- GROUP_CONCAT(DISTINCT IF(lm.renting_in_country IS NULL OR lm.renting_in_country = '', NULL, lm.renting_in_country)) AS renting_in_country_list,
                SUBSTRING_INDEX(GROUP_CONCAT(DISTINCT IF(lm.renting_in_country IS NULL OR lm.renting_in_country = '', NULL, lm.renting_in_country)), ',', 1) AS renting_in_country, -- first non null renting in country

                -- GROUP_CONCAT(DISTINCT IF(ls.source_name IS NULL OR ls.source_name = '', NULL, ls.source_name)) AS source_name_list,
                SUBSTRING_INDEX(GROUP_CONCAT(DISTINCT IF(ls.source_name IS NULL OR ls.source_name = '', NULL, ls.source_name)), ',', 1) AS source_name,
        
                SUBSTRING_INDEX(GROUP_CONCAT(DISTINCT IF(bm.booking_created_on IS NULL OR bm.booking_created_on = '', NULL, bm.booking_created_on)), ',', 1) AS booking_created_on_utc,

                COUNT(lm.lead_id) AS count_lead_id,
                'Multiple Leads per Booking' AS query_source

            FROM leads_master AS lm
                LEFT JOIN booking_master AS bm ON lm.app_booking_id = bm.Booking_id
                LEFT JOIN lead_status AS st ON lm.lead_status_id = st.id
                LEFT JOIN lead_sources AS ls ON lm.lead_source_id = ls.id

            WHERE 
                DATE_FORMAT(lm.created_on, '%Y-%m-%d') >= DATE_FORMAT(DATE_ADD(NOW(), INTERVAL 5 HOUR), '%Y-%m-%d') - INTERVAL ${date_interval} DAY
                AND (
                    lm.lead_status_id NOT IN (12, 13, 14) OR 
                    (
                        COALESCE(bm.promo_code, '') NOT IN (SELECT promo_code FROM conversion_excluded_promo_codes WHERE is_active = 1) 
                        OR COALESCE(bm.promo_code, '') = '' 
                        OR TIMESTAMPDIFF(DAY, lm.created_on, CONVERT_TZ(bm.booking_created_on, '+00:00', '+05:00')) <= 7
                    )
                )
            GROUP BY DATE_FORMAT(lm.created_on, '%Y-%m-%d'), bm.Booking_id
            HAVING bm.Booking_id IS NOT NULL AND count_lead_id > 1

            UNION ALL

            -- Second subquery: Handles cases where a single lead is associated with a booking
            SELECT
                DATE_FORMAT(lm.created_on, '%Y-%m-%d') AS created_on_pst,
                bm.Booking_id AS booking_id,

                SUBSTRING_INDEX(GROUP_CONCAT(DISTINCT IF(bm.rental_status IS NULL OR bm.rental_status = '', NULL, bm.rental_status)), ',', 1) AS rental_status,
                SUBSTRING_INDEX(GROUP_CONCAT(DISTINCT IF(lm.lead_status_id IS NULL OR lm.lead_status_id = '', NULL, lm.lead_status_id)), ',', 1) AS lead_status_id,

                -- GROUP_CONCAT(DISTINCT IF(lm.lead_id IS NULL OR lm.lead_id = '', NULL, lm.lead_id)) AS lead_id_list,
                SUBSTRING_INDEX(GROUP_CONCAT(DISTINCT IF(lm.lead_id IS NULL OR lm.lead_id = '', NULL, lm.lead_id)), ',', 1) AS lead_id,
            
                -- GROUP_CONCAT(DISTINCT IF(lm.renting_in_country IS NULL OR lm.renting_in_country = '', NULL, lm.renting_in_country)) AS renting_in_country_list,
                SUBSTRING_INDEX(GROUP_CONCAT(DISTINCT IF(lm.renting_in_country IS NULL OR lm.renting_in_country = '', NULL, lm.renting_in_country)), ',', 1) AS renting_in_country,
                
                -- GROUP_CONCAT(DISTINCT IF(ls.source_name IS NULL OR ls.source_name = '', NULL, ls.source_name)) AS source_name_list,
                SUBSTRING_INDEX(GROUP_CONCAT(DISTINCT IF(ls.source_name IS NULL OR ls.source_name = '', NULL, ls.source_name)), ',', 1) AS source_name,

                SUBSTRING_INDEX(GROUP_CONCAT(DISTINCT IF(bm.booking_created_on IS NULL OR bm.booking_created_on = '', NULL, bm.booking_created_on)), ',', 1) AS booking_created_on_utc,

                COUNT(lm.lead_id) AS count_lead_id,
                'Single Lead per Booking' AS query_source

            FROM leads_master AS lm
                LEFT JOIN booking_master AS bm ON lm.app_booking_id = bm.Booking_id
                LEFT JOIN lead_status AS st ON lm.lead_status_id = st.id
                LEFT JOIN lead_sources AS ls ON lm.lead_source_id = ls.id

            WHERE 
                DATE_FORMAT(lm.created_on, '%Y-%m-%d') >= DATE_FORMAT(DATE_ADD(NOW(), INTERVAL 5 HOUR), '%Y-%m-%d') - INTERVAL ${date_interval} DAY
                AND (
                    lm.lead_status_id NOT IN (12, 13, 14) OR 
                    (
                        COALESCE(bm.promo_code, '') NOT IN (SELECT promo_code FROM conversion_excluded_promo_codes WHERE is_active = 1) 
                        OR COALESCE(bm.promo_code, '') = '' 
                        OR TIMESTAMPDIFF(DAY, lm.created_on, CONVERT_TZ(bm.booking_created_on, '+00:00', '+05:00')) <= 7
                    )
                )
            GROUP BY DATE_FORMAT(lm.created_on, '%Y-%m-%d'), bm.Booking_id
            HAVING bm.Booking_id IS NOT NULL AND count_lead_id = 1

            UNION ALL

            -- Third subquery: Handles cases where leads do not have associated bookings
            SELECT
                DATE_FORMAT(lm.created_on, '%Y-%m-%d') AS created_on_pst,
                bm.Booking_id AS booking_id,

                bm.rental_status,
                lm.lead_status_id,

                lm.lead_id,

                lm.renting_in_country,
                ls.source_name,

                bm.booking_created_on AS booking_created_on_utc,

                COUNT(lm.lead_id) AS count_lead_id,
                'No Booking' AS query_source

            FROM leads_master AS lm
                LEFT JOIN booking_master AS bm ON lm.app_booking_id = bm.Booking_id
                LEFT JOIN lead_status AS st ON lm.lead_status_id = st.id
                LEFT JOIN lead_sources AS ls ON lm.lead_source_id = ls.id
            WHERE 
                DATE_FORMAT(lm.created_on, '%Y-%m-%d') >= DATE_FORMAT(DATE_ADD(NOW(), INTERVAL 5 HOUR), '%Y-%m-%d') - INTERVAL ${date_interval} DAY
                AND bm.Booking_id IS NULL
                AND (
                    lm.lead_status_id NOT IN (12, 13, 14) OR 
                    (
                        COALESCE(bm.promo_code, '') NOT IN (SELECT promo_code FROM conversion_excluded_promo_codes WHERE is_active = 1) 
                        OR COALESCE(bm.promo_code, '') = '' 
                        OR TIMESTAMPDIFF(DAY, lm.created_on, CONVERT_TZ(bm.booking_created_on, '+00:00', '+05:00')) <= 7
                    )
                )
            GROUP BY DATE_FORMAT(lm.created_on, '%Y-%m-%d'), bm.Booking_id, lm.lead_id, lm.renting_in_country, ls.source_name, bm.rental_status, lm.lead_status_id, bm.booking_created_on
        ) AS combined_results
        GROUP BY created_on_pst, 2, 3, 4
        ORDER BY created_on_pst DESC
        ;
    -- *******************************************
    `;
}

module.exports = {
    query_lead_stats,
}