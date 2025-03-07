async function query_all_lead_data() {
    return `
        -- C:\Users\calla\development\ezhire\mysql_queries\leads\discovery_leads_responses_query_summary_v3_121324.sql
        -- this query groups by booking id if there is a booking id & group concats the fields to ensure each field is only counted as one row (not multiple rows); this handles duplicates
        -- this query groups by booking id if there is a booking id & group concats the fields to ensure each field is only counted as one row (not multiple rows); this handles duplicates
        
        SELECT 
            DATE_FORMAT(lm.created_on, '%Y-%m-%d') AS created_on_pst_lm,
            bm.Booking_id AS booking_id_bm,

            -- LEAD ID
            GROUP_CONCAT(DISTINCT IF(lm.lead_id IS NULL OR lm.lead_id = '', NULL, lm.lead_id)) AS lead_id_lm_list,
            SUBSTRING_INDEX(GROUP_CONCAT(DISTINCT IF(lm.lead_id IS NULL OR lm.lead_id = '', NULL, lm.lead_id)), ',', 1) AS lead_id,

            -- LEAD CREATED ON DATES 
            GROUP_CONCAT(lm.created_on) AS created_on_pst_lm_list, -- Already in PST
            SUBSTRING_INDEX(GROUP_CONCAT(DISTINCT IF(lm.created_on IS NULL OR lm.created_on = '', NULL, lm.created_on)), ',', 1) AS created_on_timestamp_pst_lm,

            -- -- BOOKING CREATED ON DATES
            GROUP_CONCAT(bm.booking_created_on) AS created_on_utc_bm_list,
            SUBSTRING_INDEX(GROUP_CONCAT(DISTINCT IF(bm.booking_created_on IS NULL OR bm.booking_created_on = '', NULL, bm.booking_created_on)), ',', 1) AS booking_created_on_utc_bm,

            GROUP_CONCAT(CONVERT_TZ(bm.booking_created_on, '+00:00', '+05:00')) AS created_on_pst_bm_list, -- Adjust UTC to PST
            SUBSTRING_INDEX(GROUP_CONCAT(DISTINCT IF(bm.booking_created_on IS NULL OR bm.booking_created_on = '', NULL, CONVERT_TZ(bm.booking_created_on, '+00:00', '+05:00'))), ',', 1) AS booking_created_on_pst_bm,

            -- STATUS
            SUBSTRING_INDEX(GROUP_CONCAT(DISTINCT IF(bm.rental_status IS NULL OR bm.rental_status = '', NULL, bm.rental_status)), ',', 1) AS rental_status, -- first non null rental status,    
            SUBSTRING_INDEX(GROUP_CONCAT(DISTINCT IF(lm.lead_status_id IS NULL OR lm.lead_status_id = '', NULL, lm.lead_status_id)), ',', 1) AS lead_status_id, -- first non null lead status id
             
            SUBSTRING_INDEX(GROUP_CONCAT(DISTINCT IF(bm.rental_status IS NULL OR bm.rental_status = '', NULL, rs.status)), ',', 1) AS rental_status_desc, -- first non null rental status,  
            SUBSTRING_INDEX(GROUP_CONCAT(DISTINCT IF(lm.lead_status_id IS NULL OR lm.lead_status_id = '', NULL, st.lead_status)), ',', 1) AS lead_status_desc, -- first non null lead status id

            -- RENTING IN COUNTRY
            SUBSTRING_INDEX(GROUP_CONCAT(DISTINCT
                CASE 
                    WHEN bm.country IS NOT NULL AND bm.country <> '' THEN bm.country
                    WHEN lm.renting_in_country IS NOT NULL AND lm.renting_in_country <> '' THEN lm.renting_in_country
                    ELSE NULL
            END), ',', 1) AS renting_in_country, -- first non null renting in country
            SUBSTRING_INDEX(GROUP_CONCAT(DISTINCT
                CASE 
                    WHEN bm.country IS NOT NULL AND bm.country <> '' AND LOWER(bm.country) = 'united arab emirates' THEN 'uae'
                    WHEN bm.country IS NOT NULL AND bm.country <> '' AND LOWER(bm.country) <> 'united arab emirates' THEN LOWER(LEFT(bm.country, 3))
                    WHEN lm.renting_in_country IS NOT NULL AND lm.renting_in_country <> '' AND LOWER(lm.renting_in_country) = 'united arab emirates' THEN 'uae'
                    WHEN lm.renting_in_country IS NOT NULL AND lm.renting_in_country <> '' AND LOWER(lm.renting_in_country) <> 'united arab emirates' THEN LOWER(LEFT(lm.renting_in_country, 3))
                    ELSE 'Unknown'
            END), ',', 1) AS renting_in_country_abb, -- first non null renting in country

            -- RENTING IN COUNTRY LEAD MASTER
            GROUP_CONCAT(DISTINCT IF(lm.renting_in_country IS NULL OR lm.renting_in_country = '', NULL, lm.renting_in_country)) AS renting_in_country_list_lm, 
		    SUBSTRING_INDEX(GROUP_CONCAT(DISTINCT IF(lm.renting_in_country IS NULL OR lm.renting_in_country = '', NULL, lm.renting_in_country)), ',', 1) AS renting_in_country_lm, -- first non null renting in country

            -- RENTING IN COUNTRY BOOKING MASTER
            GROUP_CONCAT(DISTINCT IF(bm.country IS NULL OR bm.country = '', NULL, bm.country)) AS country_list_bm, 
		    SUBSTRING_INDEX(GROUP_CONCAT(DISTINCT IF(bm.country IS NULL OR bm.country = '', NULL, bm.country)), ',', 1) AS country_bm, -- first non null renting in country

            -- SOURCE NAME LEAD MASTER
            GROUP_CONCAT(DISTINCT IF(ls.source_name IS NULL OR ls.source_name = '', NULL, ls.source_name)) AS source_name_list_lm,
            SUBSTRING_INDEX(GROUP_CONCAT(DISTINCT IF(ls.source_name IS NULL OR ls.source_name = '', NULL, ls.source_name)), ',', 1) AS source_name_lm,

            -- MIN DATE - LEAD MASTER CREATED ON & CALL LOG CREATED ON
            MIN(DATE_FORMAT(lm.created_on, '%Y-%m-%d'))AS min_lead_created_on_pst,
            GROUP_CONCAT(DISTINCT cl.min_created_on_cl) AS min_created_on_pst_list_cl,
            SUBSTRING_INDEX(GROUP_CONCAT(DISTINCT IF(cl.min_created_on_cl IS NULL OR cl.min_created_on_cl = '', NULL, cl.min_created_on_cl)), ',', 1) AS min_created_on_pst_cl,

            -- RESPONSE TIME
            CONCAT(
                FLOOR(TIMESTAMPDIFF(SECOND, MIN(lm.created_on), MIN(cl.min_created_on_cl)) / 3600), ':', -- Hours
                LPAD(FLOOR(MOD(TIMESTAMPDIFF(SECOND, MIN(lm.created_on), MIN(cl.min_created_on_cl)), 3600) / 60), 2, '0'), ':', -- Minutes
                LPAD(MOD(TIMESTAMPDIFF(SECOND, MIN(lm.created_on), MIN(cl.min_created_on_cl)), 60), 2, '0') -- Seconds
            ) AS response_time,
            
            -- Response Time Binning
            CONCAT(
				CASE
                    WHEN MIN(cl.min_created_on_cl) IS NULL THEN '0) No response time'
                    WHEN TIMESTAMPDIFF(MINUTE, MIN(lm.created_on), MIN(cl.min_created_on_cl)) <= 2 THEN '1) 0-2 minutes'
                    WHEN TIMESTAMPDIFF(MINUTE, MIN(lm.created_on), MIN(cl.min_created_on_cl)) BETWEEN 3 AND 5 THEN '2) 3-5 minutes'
                    WHEN TIMESTAMPDIFF(MINUTE, MIN(lm.created_on), MIN(cl.min_created_on_cl)) BETWEEN 6 AND 10 THEN '3) 6-10 minutes'
                    WHEN TIMESTAMPDIFF(MINUTE, MIN(lm.created_on), MIN(cl.min_created_on_cl)) BETWEEN 11 AND 15 THEN '4) 11-15 minutes'
                ELSE '5) > 15 minutes'
            END) AS response_time_bin,

            -- SHIFT BASED ON CREATED TIME
            GROUP_CONCAT(DISTINCT
                CASE 
                    WHEN TIME(lm.created_on) BETWEEN '00:00:00' AND '07:59:59' THEN 'AM: 12a-8a'
                    WHEN TIME(lm.created_on) BETWEEN '08:00:00' AND '15:59:59' THEN 'Day: 8a-4p'
                    WHEN TIME(lm.created_on) BETWEEN '16:00:00' AND '23:59:59' THEN 'Night: 4p-12a'
                ELSE NULL
            END) AS shift_list,
            SUBSTRING_INDEX(GROUP_CONCAT(DISTINCT
                CASE 
                    WHEN TIME(lm.created_on) BETWEEN '00:00:00' AND '07:59:59' THEN 'AM: 12a-8a'
                    WHEN TIME(lm.created_on) BETWEEN '08:00:00' AND '15:59:59' THEN 'Day: 8a-4p'
                    WHEN TIME(lm.created_on) BETWEEN '16:00:00' AND '23:59:59' THEN 'Night: 4p-12a'
                ELSE NULL
            END), ',', 1) AS shift,
                        
            -- MAX CREATED ON FOR ALL RECORDS
            (	
                SELECT 
                    DATE_SUB(DATE_FORMAT(MAX(created_on), '%Y-%m-%d %H:%i:%s'), INTERVAL 1 HOUR) -- convert pst to gst
                FROM leads_master 
                LIMIT 1
            ) AS max_created_on_gst,
            
            COUNT(bm.Booking_id) AS count_bookings, -- included duplicates thus not valid
            COUNT(lm.lead_id) AS count_leads -- included duplicates thus not valid

        FROM leads_master AS lm
            LEFT JOIN booking_master AS bm ON lm.app_booking_id = bm.Booking_id
			LEFT JOIN rental_status AS rs ON bm.rental_status = rs.id
			LEFT JOIN lead_status AS st ON lm.lead_status_id = st.id
			LEFT JOIN lead_sources AS ls ON lm.lead_source_id = ls.id
            -- this join finds the min created on call log for each lead id (& makes the query more efficent)
            LEFT JOIN (
                SELECT 
                    Lead_id,
                    MIN(Created_On) AS min_created_on_cl
                FROM lead_aswat_Call_Logs
                GROUP BY Lead_id
            ) AS cl ON lm.lead_id = cl.Lead_id

        -- WHERE DATE
        WHERE 
            -- DATE(lm.created_on) = '2024-12-04' AND 
            YEAR(lm.created_on) >= '2024' AND
            (
                lm.lead_status_id NOT IN (12, 13, 14) OR 
                (
                    COALESCE(bm.promo_code, '') NOT IN (SELECT promo_code FROM conversion_excluded_promo_codes WHERE is_active = 1) 
                    OR COALESCE(bm.promo_code, '') = '' 
                    OR TIMESTAMPDIFF(DAY, lm.created_on, CONVERT_TZ(bm.booking_created_on, '+00:00', '+05:00')) <= 7
                )
            )
        -- this group by groups by booking id when there is a booking id (thus ensuring booking ids assigned to more than 1 lead are only counted once for the booking id and lead id)
        -- if there is no booking id then the it is grouped by lead id
        GROUP BY 
            CASE 
                WHEN bm.Booking_id IS NOT NULL THEN bm.Booking_id 
                ELSE lm.lead_id
            END,
            created_on_pst_lm,
            booking_id_bm
    `;
}

module.exports = {
    query_all_lead_data,
}