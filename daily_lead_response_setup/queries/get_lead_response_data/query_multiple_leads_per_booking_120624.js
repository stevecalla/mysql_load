const date_interval = 3;
const date = '2024-12-05';

async function query_multiple_leads_per_booking(interval = date_interval) {
    return `
        -- #2) First subquery: Handles cases where multiple leads are associated with a single booking
            SELECT
                DATE_FORMAT(lm.created_on, '%Y-%m-%d') AS created_on_pst,
                NULLIF(bm.Booking_id, '') AS booking_id,
            
                SUBSTRING_INDEX(GROUP_CONCAT(DISTINCT IF(bm.rental_status IS NULL OR bm.rental_status = '', NULL, bm.rental_status)), ',', 1) AS rental_status, -- first non null rental_status
                SUBSTRING_INDEX(GROUP_CONCAT(DISTINCT IF(lm.lead_status_id IS NULL OR lm.lead_status_id = '', NULL, lm.lead_status_id)), ',', 1) AS lead_status_id, -- first non null lead status id
                
                -- GROUP_CONCAT(DISTINCT IF(lm.lead_id IS NULL OR lm.lead_id = '', NULL, lm.lead_id)) AS lead_id_list,
                SUBSTRING_INDEX(GROUP_CONCAT(DISTINCT IF(lm.lead_id IS NULL OR lm.lead_id = '', NULL, lm.lead_id)), ',', 1) AS lead_id, -- first non null lead_id
                
                -- GROUP_CONCAT(DISTINCT IF(lm.renting_in_country IS NULL OR lm.renting_in_country = '', NULL, lm.renting_in_country)) AS renting_in_country_list,
                SUBSTRING_INDEX(GROUP_CONCAT(DISTINCT IF(lm.renting_in_country IS NULL OR lm.renting_in_country = '', NULL, lm.renting_in_country)), ',', 1) AS renting_in_country, -- first non null renting in country
            
                -- GROUP_CONCAT(DISTINCT IF(ls.source_name IS NULL OR ls.source_name = '', NULL, ls.source_name)) AS source_name_list,
                SUBSTRING_INDEX(GROUP_CONCAT(DISTINCT IF(ls.source_name IS NULL OR ls.source_name = '', NULL, ls.source_name)), ',', 1) AS source_name, -- first non null source name

                SUBSTRING_INDEX(GROUP_CONCAT(DISTINCT IF(bm.booking_created_on IS NULL OR bm.booking_created_on = '', NULL, bm.booking_created_on)), ',', 1) AS booking_created_on_utc,

                -- COUNT(lm.lead_id) AS count_lead_id,
                COUNT(DISTINCT lm.lead_id) AS count_lead_id

                        -- Response Time
                , MIN(lm.created_on) AS min_lead_created_on_pst
                , MIN(acl.Created_On) AS min_call_log_min_created_on_pst
                , CONCAT(
                        FLOOR(TIMESTAMPDIFF(SECOND, MIN(lm.created_on), MIN(acl.Created_On)) / 3600), ':', -- Hours
                        LPAD(FLOOR(MOD(TIMESTAMPDIFF(SECOND, MIN(lm.created_on), MIN(acl.Created_On)), 3600) / 60), 2, '0'), ':', -- Minutes
                        LPAD(MOD(TIMESTAMPDIFF(SECOND, MIN(lm.created_on), MIN(acl.Created_On)), 60), 2, '0') -- Seconds
                ) AS response_time
                
                -- Response Time
                , MIN(lm.created_on) AS min_lead_created_on_pst
                , MIN(acl.Created_On) AS min_call_log_min_created_on_pst
                , CONCAT(
                        FLOOR(TIMESTAMPDIFF(SECOND, MIN(lm.created_on), MIN(acl.Created_On)) / 3600), ':', -- Hours
                        LPAD(FLOOR(MOD(TIMESTAMPDIFF(SECOND, MIN(lm.created_on), MIN(acl.Created_On)), 3600) / 60), 2, '0'), ':', -- Minutes
                        LPAD(MOD(TIMESTAMPDIFF(SECOND, MIN(lm.created_on), MIN(acl.Created_On)), 60), 2, '0') -- Seconds
                ) AS response_time
                    
                -- Response Time Binning
                , CASE
                    WHEN MIN(acl.Created_On) IS NULL THEN '0) No response data'
                    WHEN TIMESTAMPDIFF(MINUTE, MIN(lm.created_on), MIN(acl.Created_On)) <= 2 THEN '1) 0-2 minutes'
                    WHEN TIMESTAMPDIFF(MINUTE, MIN(lm.created_on), MIN(acl.Created_On)) BETWEEN 3 AND 5 THEN '2) 3-5 minutes'
                    WHEN TIMESTAMPDIFF(MINUTE, MIN(lm.created_on), MIN(acl.Created_On)) BETWEEN 6 AND 10 THEN '3) 6-10 minutes'
                    WHEN TIMESTAMPDIFF(MINUTE, MIN(lm.created_on), MIN(acl.Created_On)) BETWEEN 11 AND 15 THEN '4) 11-15 minutes'
                    ELSE '5) 15+ minutes'
                END AS response_time_bin

                -- SHIFT BASED ON CREATED TIME              
                , CASE 
                        WHEN (CAST(lm.created_on AS TIME) BETWEEN '00:00:00' AND '07:59:59') THEN 'AM: 12a-8a'
                        WHEN (CAST(lm.created_on AS TIME) BETWEEN '08:00:00' AND '15:59:59') THEN 'Day: 8a-4p '
                        WHEN (CAST(lm.created_on AS TIME) BETWEEN '16:00:00' AND '23:59:59') THEN 'Night: 4p-12a'
                        ELSE NULL
                END AS shift

                , 'Multiple Leads per Booking' AS query_source, 

                -- Max created_on for all records (without per-grouping)
                (	
                    SELECT 
                        DATE_SUB(DATE_FORMAT(MAX(created_on), '%Y-%m-%d %H:%i:%s'), INTERVAL 1 HOUR) -- convert pst to gst
                    FROM leads_master 
                    LIMIT 1
                ) AS max_created_on_gst

            FROM leads_master AS lm
                LEFT JOIN booking_master AS bm ON lm.app_booking_id = bm.Booking_id
                LEFT JOIN lead_status AS st ON lm.lead_status_id = st.id
                LEFT JOIN lead_sources AS ls ON lm.lead_source_id = ls.id
                LEFT JOIN lead_aswat_Call_Logs acl ON lm.lead_id = acl.Lead_id

            WHERE 
                -- DATE_FORMAT(lm.created_on, '%Y-%m-%d') = '${date}' -- UTC to PST (Pakistan Standard Time)
                DATE_FORMAT(lm.created_on, '%Y-%m-%d') >= DATE_FORMAT(DATE_ADD(UTC_TIMESTAMP(), INTERVAL 5 HOUR), '%Y-%m-%d') - INTERVAL ${interval} DAY
                
                -- To remove marketing promo leads
                AND 
                (
                    lm.lead_status_id NOT IN (12, 13, 14) OR 
                    (
                        COALESCE(bm.promo_code, '') NOT IN (SELECT promo_code FROM conversion_excluded_promo_codes WHERE is_active = 1) 
                        OR COALESCE(bm.promo_code, '') = '' 
                        OR TIMESTAMPDIFF(DAY, lm.created_on, CONVERT_TZ(bm.booking_created_on, '+00:00', '+05:00')) <= 7
                    )
                )
            GROUP BY DATE_FORMAT(lm.created_on, '%Y-%m-%d'), bm.Booking_id, shift
            HAVING 
                bm.Booking_id IS NOT NULL
                AND count_lead_id > 1
            ORDER BY DATE_FORMAT(lm.created_on, '%Y-%m-%d') DESC
            ;
        -- *******************************************
    `;
}

module.exports = {
    query_multiple_leads_per_booking,
}