const query_create_key_metrics_rollup = `

    SET SESSION group_concat_max_len = 100000;  -- Increase limit

    CREATE TABLE user_data_key_metrics_rollup AS
        SELECT 
            user_ptr_id,
            -- booking_id, -- to see the detail for each booking_id,
            -- status, -- to see the detail for each booking_id,

            -- COUNTRY & CITY STATS
            --  GROUP_CONCAT(DISTINCT deliver_country ORDER BY deliver_country ASC SEPARATOR ', ') AS all_countries_distinct,
            --  GROUP_CONCAT(DISTINCT deliver_city ORDER BY deliver_city ASC SEPARATOR ', ') AS all_cities_distinct,
            GROUP_CONCAT(DISTINCT CASE WHEN status NOT LIKE '%cancelled%' THEN deliver_country END ORDER BY deliver_country ASC SEPARATOR ', ') AS all_countries_distinct,
            GROUP_CONCAT(DISTINCT CASE WHEN status NOT LIKE '%cancelled%' THEN deliver_city END ORDER BY deliver_city ASC SEPARATOR ', ') AS all_cities_distinct,

            -- PROMO CODE STATS -- TODO:
            -- (a) list of a promo codes
            GROUP_CONCAT(DISTINCT CASE WHEN status NOT LIKE '%cancelled%' THEN promo_code END ORDER BY booking_date ASC SEPARATOR ', ') AS all_promo_codes_distinct,
            -- (b) most recent booking promo code; if a no promo code on last booking it will be blank
            -- (b.1) if used a promo code on last rental... code below also covers this as it will be blank if promo not used on most recent booking
            (
                SELECT 
                    inner_data.promo_code
                FROM 
                    ezhire_user_data.user_data_combined_booking_data AS inner_data
                WHERE 
                    inner_data.user_ptr_id = user_data_combined_booking_data.user_ptr_id
                    AND inner_data.return_date = 
                    (
                        SELECT 
                            MAX(inner_data2.return_date)
                        FROM ezhire_user_data.user_data_combined_booking_data AS inner_data2
                        WHERE 
                            inner_data2.user_ptr_id = inner_data.user_ptr_id
                            AND inner_data2.status NOT IN ('Cancelled by User')
                    )
                    AND inner_data.status NOT IN ('Cancelled by User')
                ORDER BY inner_data.return_date DESC
                LIMIT 1
            ) AS promo_code_on_most_recent_booking,

            -- (c) used a promo code in the last 14 days
            (
                SELECT 
                    CASE 
                        WHEN MAX(inner_data.return_date) >= (CURDATE() - INTERVAL 14 DAY) THEN 'Yes' 
                        ELSE 'No' 
                    END AS used_promo_code_last_14_days_flag
                FROM 
                    ezhire_user_data.user_data_combined_booking_data AS inner_data
                WHERE 
                    inner_data.user_ptr_id = user_data_combined_booking_data.user_ptr_id
                    AND inner_data.status NOT IN ('Cancelled by User')
                LIMIT 1
            ) AS used_promo_code_last_14_days_flag,

            -- (d) only used a promo code, 
            (
                SELECT 
                    CASE 
                        WHEN COUNT(inner_data.promo_code) = 0 THEN 'No' 
                        WHEN COUNT(inner_data.promo_code) = COUNT(*) THEN 'Yes' 
                        ELSE 'No' 
                    END AS used_promo_code_on_every_booking
                FROM 
                    ezhire_user_data.user_data_combined_booking_data AS inner_data
                WHERE 
                    inner_data.user_ptr_id = user_data_combined_booking_data.user_ptr_id
                    AND inner_data.status NOT IN ('Cancelled by User')
            ) AS used_promo_code_on_every_booking,

            -- BOOKING TYPE
                --  GROUP_CONCAT(DISTINCT booking_type ORDER BY booking_type ASC SEPARATOR ', ') AS booking_type_all_distinct,
                GROUP_CONCAT(DISTINCT CASE WHEN status NOT LIKE '%cancelled%' THEN booking_type END ORDER BY booking_type ASC SEPARATOR ', ') AS booking_type_all_distinct,
            (SELECT booking_type
            FROM ezhire_user_data.user_data_combined_booking_data AS inner_data
            WHERE inner_data.user_ptr_id = user_data_combined_booking_data.user_ptr_id
            AND inner_data.return_date = (
                SELECT MAX(return_date)
                FROM ezhire_user_data.user_data_combined_booking_data AS inner_data2
                WHERE inner_data2.user_ptr_id = inner_data.user_ptr_id
                    AND inner_data2.status NOT IN ('Cancelled by User')
            )
            AND inner_data.status NOT IN ('Cancelled by User')
            LIMIT 1) AS booking_type_most_recent,

            -- CANCELLER, REPEAT, NEW, FIRST
            CASE
                -- all bookings cancelled
                WHEN COUNT(booking_id) <> 0 AND COUNT(booking_id) = SUM(CASE WHEN status LIKE '%cancelled%' THEN 1 ELSE 0 END) THEN 'canceller'
                -- REPEAT = multiple bookings
                WHEN SUM(CASE WHEN status NOT LIKE '%cancelled%' THEN 1 ELSE 0 END) >= 2 THEN 'repeat' 
                -- booking within 48 hours of join datetime
                WHEN DATEDIFF(MIN(booking_date), MIN(date_join_formatted_gst)) <= 2 AND SUM(CASE WHEN status NOT LIKE '%cancelled%' THEN 1 ELSE 0 END) = 1 THEN 'new' 
                -- FIRST = 1 booking after 48 hours of join datetime
                WHEN DATEDIFF(MIN(booking_date), MIN(date_join_formatted_gst)) > 2 AND SUM(CASE WHEN status NOT LIKE '%cancelled%' THEN 1 ELSE 0 END) = 1 THEN 'first' 
                -- LOOKER
                WHEN COUNT(booking_id) = 0 THEN 'looker'
                ELSE 'other'
            END AS is_repeat_new_first,

            -- BOOKING COUNT STATS
            COUNT(booking_id) AS booking_count_total,
            SUM(CASE WHEN status LIKE '%cancelled%' THEN 1 ELSE 0 END) AS booking_count_cancel,
            SUM(CASE WHEN status LIKE '%ended%' THEN 1 ELSE 0 END) AS booking_count_completed,
            SUM(CASE WHEN status LIKE '%started%' THEN 1 ELSE 0 END) AS booking_count_started,
            SUM(CASE WHEN status LIKE '%future%' THEN 1 ELSE 0 END) AS booking_count_future,
            SUM(CASE WHEN status NOT LIKE '%cancelled%' AND status NOT LIKE '%ended%' AND status NOT LIKE '%started%' AND status NOT LIKE '%future%' THEN 1 ELSE 0 END) AS booking_count_other,
            SUM(CASE WHEN status NOT LIKE '%cancelled%' THEN 1 ELSE 0 END) AS booking_count_not_cancel,

            -- REVENUE STATS
            SUM(CASE WHEN status NOT LIKE '%cancelled%' THEN booking_charge_less_discount_aed ELSE 0 END) AS booking_charge_total_less_discount_aed, 
            SUM(CASE WHEN status NOT LIKE '%cancelled%' THEN booking_charge_less_discount_extension_aed ELSE 0 END) AS booking_charge_total_less_discount_extension_aed,
            SUM(CASE WHEN status NOT LIKE '%cancelled%' THEN extension_charge_aed ELSE 0 END) AS booking_charge_extension_only_aed,

            -- DAYS STATS
            SUM(CASE WHEN status NOT LIKE '%cancelled%' THEN days ELSE 0 END) AS booking_days_total,
            SUM(CASE WHEN status NOT LIKE '%cancelled%' THEN (days - extension_days) ELSE 0 END) AS booking_days_initial_only,
            SUM(CASE WHEN status NOT LIKE '%cancelled%' THEN extension_days ELSE 0 END) AS booking_days_extension_only,

            -- MOST RECENT DATES (cast as a date as default is varchar, excluded cancelled bookings)
            CASE
                WHEN MIN(CASE WHEN status NOT LIKE '%cancelled%' THEN booking_date ELSE NULL END) IS NULL THEN NULL
                ELSE CAST(IFNULL(MIN(CASE WHEN status NOT LIKE '%cancelled%' THEN booking_date ELSE NULL END), '') AS DATE)
            END AS booking_first_created_date,
            CASE
                WHEN MAX(CASE WHEN status NOT LIKE '%cancelled%' THEN booking_date ELSE NULL END) IS NULL THEN NULL
                ELSE CAST(IFNULL(MAX(CASE WHEN status NOT LIKE '%cancelled%' THEN booking_date ELSE NULL END), '') AS DATE)
            END AS booking_most_recent_created_date,
            CASE
                WHEN MAX(CASE WHEN status NOT LIKE '%cancelled%' THEN pickup_date ELSE NULL END) IS NULL THEN NULL
                ELSE CAST(IFNULL(MAX(CASE WHEN status NOT LIKE '%cancelled%' THEN pickup_date ELSE NULL END), '') AS DATE)
            END AS booking_most_recent_pickup_date,
            CASE
                WHEN MAX(CASE WHEN status NOT LIKE '%cancelled%' THEN return_date ELSE NULL END) IS NULL THEN NULL
                ELSE CAST(IFNULL(MAX(CASE WHEN status NOT LIKE '%cancelled%' THEN return_date ELSE NULL END), '') AS DATE)
            END AS booking_most_recent_return_date,

            -- DATE COMPARISONS (cast as a number; default is varchar, excluded cancelled bookings)
            -- CAST(
            -- 	IFNULL(TIMESTAMPDIFF(DAY, MIN(CASE WHEN status NOT LIKE '%cancelled%' THEN date_join_formatted_gst ELSE NULL END), MIN(CASE WHEN status NOT LIKE '%cancelled%' THEN booking_date ELSE NULL END)), '') 
            -- 	AS DOUBLE
            -- ) AS booking_join_vs_first_created,

            CAST(
                CASE
                    WHEN MIN(CASE WHEN status NOT LIKE '%cancelled%' THEN date_join_formatted_gst ELSE NULL END) IS NOT NULL
                        AND MIN(CASE WHEN status NOT LIKE '%cancelled%' THEN booking_date ELSE NULL END) IS NOT NULL
                        AND MIN(CASE WHEN status NOT LIKE '%cancelled%' THEN date_join_formatted_gst ELSE NULL END) > MIN(CASE WHEN status NOT LIKE '%cancelled%' THEN booking_date ELSE NULL END)
                    THEN 0  -- Set to 0 when date_join_formatted_gst is greater than booking_date
                    ELSE TIMESTAMPDIFF(
                        DAY,
                        MIN(CASE WHEN status NOT LIKE '%cancelled%' THEN date_join_formatted_gst ELSE NULL END),
                        MIN(CASE WHEN status NOT LIKE '%cancelled%' THEN booking_date ELSE NULL END)
                    )
                END AS DOUBLE
            ) AS booking_join_vs_first_created,


            CAST(
                IFNULL(TIMESTAMPDIFF(DAY, MIN(CASE WHEN status NOT LIKE '%cancelled%' THEN booking_date ELSE NULL END), MIN(CASE WHEN status NOT LIKE '%cancelled%' THEN pickup_date ELSE NULL END)), '') 
                AS DOUBLE
            ) AS booking_first_created_vs_first_pickup,
            CAST(
                IFNULL(TIMESTAMPDIFF(DAY, DATE_FORMAT(MAX(CASE WHEN status NOT LIKE '%cancelled%' THEN booking_date ELSE NULL END), '%Y-%m-%d'), DATE_FORMAT(DATE_ADD(UTC_TIMESTAMP(), INTERVAL 4 HOUR), '%Y-%m-%d')), '')
                AS DOUBLE
            ) AS booking_most_recent_created_on_vs_now,
            CAST(
                IFNULL(TIMESTAMPDIFF(DAY, DATE_FORMAT(MAX(CASE WHEN status NOT LIKE '%cancelled%' THEN return_date ELSE NULL END), '%Y-%m-%d'), DATE_FORMAT(DATE_ADD(UTC_TIMESTAMP(), INTERVAL 4 HOUR), '%Y-%m-%d')), '')
                AS DOUBLE
            ) AS booking_most_recent_return_vs_now,

            -- RESIDENT CATEGORY
            IFNULL(LEFT(GROUP_CONCAT(resident_category ORDER BY booking_date ASC SEPARATOR ','), 10000), '') AS all_resident_category, -- needed to limit the char due to errors
            IFNULL(
                SUBSTRING_INDEX(
                    GROUP_CONCAT(resident_category ORDER BY booking_date ASC SEPARATOR ','), 
                    ',', 
                    -1
                ), 
                ''
            ) AS most_recent_resident_category,

            -- NPS FIELDS
            IFNULL(GROUP_CONCAT(nps_score ORDER BY booking_date ASC SEPARATOR ','), '') AS all_nps_scores, -- todo: new
            IFNULL(
                SUBSTRING_INDEX(
                    GROUP_CONCAT(nps_score ORDER BY booking_date ASC SEPARATOR ','), 
                    ',', 
                    -1
                ), 
                ''
            ) AS most_recent_nps_score,
            IFNULL(
                SUBSTRING_INDEX(
                    GROUP_CONCAT(nps_comment ORDER BY booking_date ASC SEPARATOR ','), 
                    ',', 
                    -1
                ), 
                ''
            ) AS most_recent_nps_comment,

            -- BOOKING ID GROUPING
            IFNULL(GROUP_CONCAT(booking_id ORDER BY booking_date ASC SEPARATOR ','), '') AS all_booking_ids, -- todo: new

            -- EXTENSION SEGMENTS
            SUM(
                CASE 
                    WHEN is_extended = 'YES' THEN 1 
                    WHEN is_extended = 'NO' THEN 0 
                    ELSE 0 
                END
            ) AS booking_count_extended, -- todo:

            -- UTC NOW CONVERTED TO GST
            DATE_ADD(UTC_TIMESTAMP(), INTERVAL 4 HOUR) AS date_now_gst

        FROM ezhire_user_data.user_data_combined_booking_data

        -- TO VIEW THE BOOKING DETAIL BY ID USE THIS WHERE STATEMENT
            -- date_join_formatted_gst >= '2024-01-01'
            -- AND
            -- user_ptr_id IN ('549331') 		-- currently started renter; first created 1/4/2024, 
                                                -- most recent created on 2/23/24, most recent pickup 2/24/24, 
                                                -- most recent return 5/18/2024, most recent status rental started
            -- user_ptr_id IN ('419418') 		-- total bookings 7, completed 4, cancelled 3; repeat
            -- user_ptr_id IN ('593518', '593396') -- all cancelled renter
            -- user_ptr_id IN ('549331', '419418', '593518', '593396') -- all above first, repeat, canceller, canceller
            -- user_ptr_id IN ('537942') 			-- 537942 other count = 1; rental status = 'Vendor Assigned'
                                                -- scheduled for July 2024
            -- user_ptr_id IN ('11022')			-- recency score -196586, most recent return date '2562-08-10' & rental cancelled
            -- user_ptr_id IN ('196066') -- date join is greater than first booking date

        -- WHERE
        -- 	user_ptr_id IN ('1580')
        -- 	user_ptr_id IN ('549331') 		-- currently started renter; first created 1/4/2024, 
            -- 									most recent created on 2/23/24, most recent pickup 2/24/24, 
            -- 									most recent return 5/18/2024, most recent status rental started
            -- user_ptr_id IN ('519820') 			-- recency score -81? returned early but return date not 
                                                    -- adjusted?
            -- AND
            -- status LIKE 'Rental Ended'
            -- status NOT LIKE '%started%'
            -- status LIKE 'Rental Ended'
            -- AND
            -- return_date > UTC_TIMESTAMP()
            -- IFNULL(TIMESTAMPDIFF(DAY, DATE_FORMAT(MAX(CASE WHEN status NOT LIKE '%cancelled%' THEN return_date ELSE NULL END), '%Y-%m-%d'), DATE_FORMAT(DATE_ADD(UTC_TIMESTAMP(), INTERVAL 4 HOUR), '%Y-%m-%d')), '') < 0
        -- GROUP BY 1, 2, 3
        -- ORDER BY 1, 2;

        -- TO GET ROLLUP FOR ALL USERS
        GROUP BY 1
        ORDER BY 1;	
        -- ORDER BY 1
        -- LIMIT 100;
`;

module.exports = { query_create_key_metrics_rollup };