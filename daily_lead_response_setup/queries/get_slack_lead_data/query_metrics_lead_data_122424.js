async function query_metrics_lead_data() {
    return `
        -- C:\Users\calla\development\ezhire\mysql_queries\leads\discovery_leads_responses_query_summary_v3_121324.sql
        -- Step #1 = this query shows lead / booking counts with and without duplicates. 
        -- Duplicates exist because 1 booking is assigned to multiple lead ids.

        CREATE TABLE lead_response_metrics_data AS
            SELECT 
                CAST(created_on_pst_lm AS CHAR) AS created_on_pst,

                -- COMMENT OUT TO VIEW ROLLUP 
                IFNULL(renting_in_country, 'Unknown') AS renting_in_country,
                renting_in_country_abb,
                source_name_lm AS source_name,
                shift,
                response_time_bin,
            
                -- LEAD COUNT
                CAST(SUM(count_leads) AS UNSIGNED) AS count_lead_total_duplicates, -- included duplicates thus not valid
                CAST(IFNULL(COUNT(DISTINCT lead_id_lm_list), 0) AS UNSIGNED) AS count_leads_total, -- -- since these are grouped in the subquery, the count is accurately unique/distinct
                CAST(IFNULL(COUNT(DISTINCT CASE WHEN lead_status_id IN (16) THEN lead_id_lm_list END), 0) AS UNSIGNED) AS count_leads_invalid,
                CAST(IFNULL(COUNT(DISTINCT CASE WHEN lead_status_id NOT IN (16) THEN lead_id_lm_list END), 0) AS UNSIGNED) AS count_leads_valid,

                -- BOOKING COUNT TOTAL
                CAST(SUM(count_bookings) AS UNSIGNED) AS count_booking_total_duplicates, -- include duplicates thus not valid
                CAST(IFNULL(COUNT(DISTINCT booking_id_bm), 0) AS UNSIGNED) AS count_booking_total, -- since these are grouped in the subquery, the count is accurately unique/distinct
                CAST(IFNULL(COUNT(DISTINCT CASE WHEN lead_status_id IN (16) THEN booking_id_bm END), 0) AS UNSIGNED) AS count_booking_invalid,
                CAST(IFNULL(COUNT(DISTINCT CASE WHEN lead_status_id NOT IN (16) THEN booking_id_bm END), 0) AS UNSIGNED) AS count_booking_valid,
                
                -- BOOKING COUNT - FILTERED BY CANCELLED, INVALID, LEAD CREATED VS BOOKING CREATED
                CAST(COUNT(DISTINCT 
                    CASE 
                        WHEN 
                            booking_id_bm IS NOT NULL
                            AND rental_status NOT IN (8) -- is not cancelled
                            -- AND lead_status_id NOT IN (16) -- is valid
                            AND TIMESTAMPDIFF(DAY, created_on_pst_lm, booking_created_on_pst_bm) > 7
                            THEN booking_id_bm 
                END) AS UNSIGNED) AS count_booking_id_greater_than_7,

                CAST(COUNT(DISTINCT 
                    CASE 
                        WHEN 
                            rental_status IN (8) -- is cancelled
                            -- AND lead_status_id NOT IN (16) -- is valid
                            AND TIMESTAMPDIFF(DAY, created_on_pst_lm, booking_created_on_pst_bm) <= 7
                            THEN booking_id_bm 
                END) AS UNSIGNED) AS count_booking_id_cancelled_total,

                CAST(COUNT(DISTINCT 
                    CASE 
                        WHEN 
                            rental_status NOT IN (8) -- is not cancelled
                            -- AND lead_status_id NOT IN (16) -- is valid
                            AND TIMESTAMPDIFF(DAY, created_on_pst_lm, booking_created_on_pst_bm) <= 7
                            THEN booking_id_bm 
                END) AS UNSIGNED) AS count_booking_id_not_cancelled_total,
                
                CAST(COUNT(DISTINCT 
                    CASE 
                        WHEN 
                            booking_id_bm IS NOT NULL 
                            -- AND lead_status_id NOT IN (16) -- is valid
                            AND TIMESTAMPDIFF(DAY, created_on_pst_lm, booking_created_on_pst_bm) <= 7
                            THEN booking_id_bm 
                END) AS UNSIGNED) AS count_booking_id_filtered_total,

                -- SAME DAY BOOKING COUNTS - FILTERED BY CANCELLED, INVALID, LEAD CREATED VS BOOKING CREATED
                CAST(COUNT(DISTINCT 
                    CASE 
                        WHEN 
                            rental_status IN (8) -- is cancelled
                            -- AND lead_status_id NOT IN (16) -- is valid
                            -- AND DATE(booking_created_on_pst_bm) = DATE(created_on_pst_lm)
                            AND TIMESTAMPDIFF(DAY, created_on_pst_lm, DATE(booking_created_on_pst_bm)) < 1
                            THEN booking_id_bm 
                END) AS UNSIGNED) AS count_booking_same_day_rental_status_cancelled_distinct,

                CAST(COUNT(DISTINCT 
                    CASE 
                        WHEN 
                            rental_status NOT IN (8) -- is cancelled
                            -- AND lead_status_id NOT IN (16) -- is valid
                            -- AND DATE(booking_created_on_pst_bm) = DATE(created_on_pst_lm)
                            AND TIMESTAMPDIFF(DAY, created_on_pst_lm, DATE(booking_created_on_pst_bm)) < 1
                            THEN booking_id_bm 
                END) AS UNSIGNED) AS count_booking_same_day_rental_status_not_cancelled_distinct,
                
                CAST(COUNT(DISTINCT 
                    CASE 
                        WHEN 
                            booking_id_bm IS NOT NULL 
                            -- AND lead_status_id NOT IN (16) -- is valid
                            -- AND DATE(booking_created_on_pst_bm) = DATE(created_on_pst_lm)
                            AND TIMESTAMPDIFF(DAY, created_on_pst_lm, DATE(booking_created_on_pst_bm)) < 1
                            THEN booking_id_bm 
                END) AS UNSIGNED) AS count_booking_same_day_rental_status_distinct_total,

                -- CURRENT DATE / TIME PST (Pakistan Standard Time)
                DATE_FORMAT(UTC_TIMESTAMP(), '%Y-%m-%d %H:%i:%s') AS queried_at_utc,
                DATE_FORMAT(DATE_ADD(UTC_TIMESTAMP(), INTERVAL 4 HOUR), '%Y-%m-%d %H:%i:%s') AS queried_at_gst, -- UTC to GST
                DATE_FORMAT(DATE_ADD(UTC_TIMESTAMP(), INTERVAL 5 HOUR), '%Y-%m-%d %H:%i:%s') AS queried_at_pst, -- UTC to PST (Pakistan Standard Time)
                    
                MAX(max_created_on_gst) AS max_created_on_gst

            FROM lead_response_data

            -- COMMENT OUT TO VIEW ROLLUP STATS 
            GROUP BY 
                created_on_pst,
                renting_in_country,
                renting_in_country_abb,
                source_name_lm,
                shift,
                response_time_bin

            -- COMMENT IN TO VIEW ROLLUP STATS
            -- GROUP BY 
            --     created_on_pst DESC

            ORDER BY created_on_pst DESC 
    `;
}

module.exports = {
    query_metrics_lead_data,
}

// -- WHERE 
// -- created_on_pst_lm = '${date}'
// AND
// -- if country IS NULL evalutes to true then returns all countries
// -- else return renting_in_country = the passed country
// -- (${country === null} OR renting_in_country_abb = '${country}')