const queryBookingData = `
-- USE myproject;

-- SET @str_date = '2024-01-01', @end_date = '2024-01-01';

-- ********* START ************ CHANGE LOG
    -- CUSTOMER RATE = 0, EARLY RETURN = 1
        -- WHERE b.id IN ('208353')
	    -- WHERE b.id IN ('43646', '67222', '72671', '104647', '94785', '206269', '208353', '228349', '226414', '237124')
    -- ADD BASE FARE CODE = 35 TO other_rental_charge TO INCLUDE THE BASE FARE
        -- adjusted other_rental_charge to include 35
        -- adjusted booking_charge to include 35
        -- adjusted booking_charge_less_discount to include 35
        -- adjusted base_rental_revenue to include 35
-- ********* END *************** CHANGE LOG

SELECT 
    -- NOW(),
    -- CURDATE(),
    -- return_datetime AS test_return_date,
    -- CASE 
    --     WHEN return_datetime < now() THEN 'true'
    --     ELSE 'false'
    -- END AS test_v2,

    booking_id,
    REPLACE(REPLACE(agreement_number, '"', ''),
        ',',
        ' ') AS agreement_number,

	-- BOOKING DATE FIELDS
    IFNULL(IF(booking_datetime = '0000-00-00 00:00:00',
                NULL,
                DATE_FORMAT(booking_datetime, '%Y-%m-%d')),
            NULL) AS booking_date,
    IFNULL(IF(booking_datetime = '0000-00-00 00:00:00',
                NULL,
                DATE_FORMAT(booking_datetime, '%Y-%m-%d %H:%i:%s')),
            NULL) AS booking_datetime,

    DATE_FORMAT(max_booking_datetime, '%Y-%m-%d %H:%i:%s') AS max_booking_datetime,

    CASE
        WHEN DATE_FORMAT(booking_datetime, '%Y-%m-%d') = DATE_FORMAT(max_booking_datetime, '%Y-%m-%d') 
            THEN 'yes'
        ELSE 'no'
    END AS today,

    booking_year,
    booking_quarter,
    booking_month,
    booking_day_of_month,
    booking_week_of_year,
    booking_day_of_week,
    booking_day_of_week_v2,
    booking_time_bucket,

    -- BOOKING COUNT STATS
	1 AS booking_count,
    IF(status NOT LIKE '%Cancelled%', 1, 0) AS booking_count_excluding_cancel,
    
	-- PICKUP DATE FIELDS
    IFNULL(IF(pickup_datetime = '0000-00-00 00:00:00',
                NULL,
                DATE_FORMAT(pickup_datetime, '%Y-%m-%d')),
            NULL) AS pickup_date,
    IFNULL(IF(pickup_datetime = '0000-00-00 00:00:00',
                NULL,
                DATE_FORMAT(pickup_datetime, '%Y-%m-%d %H:%i:%s')),
            NULL) AS pickup_datetime,
    pickup_year,
    pickup_quarter,
    pickup_month,
    pickup_day_of_month,
    pickup_week_of_year,
    pickup_day_of_week,
    pickup_day_of_week_v2,
    pickup_time_bucket,

    -- RETURN DATE FIELDS = EITHER RETURN DATE OR EARLY RETURN DATE
    early_return,
    IFNULL(IF(return_datetime = '0000-00-00 00:00:00',
            NULL,
            DATE_FORMAT(return_datetime, '%Y-%m-%d')),
        NULL) AS return_date,
    IFNULL(IF(return_datetime = '0000-00-00 00:00:00',
                NULL,
                DATE_FORMAT(return_datetime, '%Y-%m-%d %H:%i:%s')),
            NULL) AS return_datetime,
    YEAR(return_datetime) AS return_year,
    QUARTER(return_datetime) AS return_quarter,
    MONTH(return_datetime) AS return_month,
    DAY(return_datetime) AS return_day_of_month, -- fix was returning full date
    WEEK(return_datetime) AS return_week_of_year,
    DAYOFWEEK(return_datetime) AS return_day_of_week,
    DAYNAME(return_datetime) AS return_day_of_week_v2,
    HOUR(return_datetime) AS return_time_bucket,
    
    -- ADVANCE CATEGORIES START
	CASE
		WHEN DAYOFMONTH(pickup_datetime) = DAYOFMONTH(booking_datetime) THEN 'SameDay'
		WHEN DAYOFMONTH(pickup_datetime) <> DAYOFMONTH(booking_datetime) THEN 'NextDay+'
		ELSE 'Other'
	END AS advance_category_day,
    
    -- assign each record a sameWeek or nextWeek field
    CASE
		WHEN WEEKOFYEAR(pickup_datetime) = WEEKOFYEAR(booking_datetime) THEN 'SameWeek'
		WHEN WEEKOFYEAR(pickup_datetime) <> WEEKOFYEAR(booking_datetime) THEN 'NextWeek+'
		ELSE 'Other'
	END AS advance_category_week,
    
    -- assign each record a sameMonth or nextMonth field
    CASE
		WHEN MONTH(pickup_datetime) = MONTH(booking_datetime) THEN 'SameMonth'
		WHEN MONTH(pickup_datetime) <> MONTH(booking_datetime) THEN 'NextMonth+'
		ELSE 'Other'
	END AS advance_category_month,
    
    -- assign each record a same day, next day, within a week, more than a week
    CASE
		WHEN DATEDIFF(pickup_datetime, booking_datetime) <= 0 THEN 'SameDay'
		WHEN DATEDIFF(pickup_datetime, booking_datetime) = 1 THEN 'NextDay'
		WHEN DATEDIFF(pickup_datetime, booking_datetime) BETWEEN 2 AND 7 THEN 'WithinAWeek'
		WHEN DATEDIFF(pickup_datetime, booking_datetime) > 7 THEN 'MoreThanAWeek'
		ELSE 'Other'
	END AS advance_category_date_within_week,
    
    -- calc the date difference between pickup and dropoff
    DATEDIFF(pickup_datetime, booking_datetime) AS advance_pickup_booking_date_diff,
    -- ADVANCE CATEGORES END

    -- COMPARISON DATES CURRENT 28 DAYS, PRIOR 4 WEEKS, 52 WEEKS PRIOR --- START
    CASE
        WHEN ((DateDiff(AddDate(Current_Date(), 0), DATE(booking_datetime)) < 28) AND (DateDiff(Current_Date(), DATE(booking_datetime)) >= 0)) THEN 'yes'
        WHEN ((DateDiff(AddDate(Current_Date(), 0), DATE(booking_datetime)) < (28 + 28)) AND (DateDiff(Current_Date(), DATE(booking_datetime)) >= 28)) THEN 'yes'
        WHEN ((DateDiff(AddDate(Current_Date(), 0),DATE(booking_datetime)) < (28 + (52 * 7))) AND (DateDiff(Current_Date(),DATE(booking_datetime)) >= (52 * 7))) THEN 'yes'
        ELSE 'no'
    END AS comparison_28_days,
    
    CASE
        WHEN ((DateDiff(AddDate(Current_Date(), 0), DATE(booking_datetime)) < 28) AND (DateDiff(Current_Date(), DATE(booking_datetime)) >= 0)) THEN 'Current_28_Days'
        WHEN ((DateDiff(AddDate(Current_Date(), 0), DATE(booking_datetime)) < (28 + 28)) AND (DateDiff(Current_Date(), DATE(booking_datetime)) >= 28)) THEN '4_Weeks_Prior'
        WHEN ((DateDiff(AddDate(Current_Date(), 0),DATE(booking_datetime)) < (28 + (52 * 7))) AND (DateDiff(Current_Date(),DATE(booking_datetime)) >= (52 * 7))) THEN '52_Weeks_Prior'
        ELSE 'other'
    END AS comparison_period,
                
    CASE  
        WHEN ((DateDiff(AddDate(Current_Date(), 0), DATE(booking_datetime)) < 28) AND (DateDiff(Current_Date(), DATE(booking_datetime)) >= 0)) THEN DATE_FORMAT(booking_datetime, '%Y-%m-%d')
        WHEN ((DateDiff(AddDate(Current_Date(), 0), DATE(booking_datetime)) < (28 + 28)) AND (DateDiff(Current_Date(), DATE(booking_datetime)) >= 28)) THEN DATE_FORMAT(DATE_ADD(DATE(booking_datetime), INTERVAL 28 DAY), '%Y-%m-%d')
        WHEN ((DateDiff(AddDate(Current_Date(), 0), DATE(booking_datetime)) < (28 + (52 * 7))) AND (DateDiff(Current_Date(), DATE(booking_datetime)) >= (52 * 7))) THEN DATE_FORMAT(DATE_ADD(DATE(booking_datetime), INTERVAL (52 * 7) DAY), '%Y-%m-%d')
    END AS comparison_common_date,
    
    CASE WHEN (DateDiff(AddDate(Current_Date(), 0), DATE(booking_datetime)) < 28) AND (DateDiff(Current_Date(), DATE(booking_datetime)) >= 0) THEN 1  ELSE 0 END AS 'Current_28_Days',

    CASE WHEN (DateDiff(AddDate(Current_Date(), 0), DATE(booking_datetime)) < (28 + 28)) AND (DateDiff(Current_Date(), DATE(booking_datetime)) >= 28) THEN 1  ELSE 0 END AS '4_Weeks_Prior',

    CASE WHEN (DateDiff(AddDate(Current_Date(), 0), DATE(booking_datetime)) < (28 + (52 * 7))) AND (DateDiff(Current_Date(), DATE(booking_datetime)) >= (52 * 7)) THEN 1  ELSE 0 END AS '52_Weeks_Prior',
    -- COMPARISON DATES CURRENT 28 DAYS, PRIOR 4 WEEKS, 52 WEEKS PRIOR --- END
    
    status,
    booking_type,
    marketplace_or_dispatch,
    marketplace_partner,
    marketplace_partner_summary,
    booking_channel,
    booking_source,
    
    repeated_user,
    total_lifetime_booking_revenue,
    no_of_bookings,
    no_of_cancel_bookings,
    no_of_completed_bookings,
    no_of_started_bookings,
    customer_id,
    
    REPLACE(first_name, ',', '') AS first_name,
    REPLACE(last_name, ',', '') AS last_name,
    REPLACE(email, ',', '') AS email,

    date_of_birth,
    age,
    
    date_join_formatted_gst,
    date_join_cohort,
    date_join_year,
    date_join_month,
    
    resident_category,

    customer_driving_country,
    customer_doc_vertification_status,
    
    days, -- adjusted for early return
    IFNULL(extension_days, 0) AS extension_days, -- ????

    IFNULL(extra_day_calc, 0) AS extra_day_calc, -- adjusted for early return
    -- IFNULL(customer_rate_v2, 0) AS customer_rate_v2, -- adjusted for early return
    IFNULL(customer_rate, 0) AS customer_rate, -- adjusted for early return
    IFNULL(insurance_rate, 0) AS insurance_rate, -- adjusted for early return

    IFNULL(additional_driver_rate, 0) AS additional_driver_rate, -- adjusted for early return
    IFNULL(pai_rate, 0) AS pai_rate,  -- adjusted for early 
    IFNULL(baby_seat_rate, 0) AS baby_seat_rate, -- adjusted for early 

    IFNULL(insurance_type, 0) AS insurance_type,
    IFNULL(millage_rate, 0) AS millage_rate,
    IFNULL(millage_cap_km, 0) AS millage_cap_km,

    -- IFNULL(rent_charge, 0) AS rent_charge, -- adjusted for early return
    IFNULL((days * customer_rate), 0) AS rent_charge,

    -- RENT CHARGE FOR INITIAL BOOKING (TO EXCLUDE EXTENSION AND DISCOUNT)
    IFNULL((((days - extension_days) * customer_rate) - (discount_charge)) * tb.conversion_rate, 0) AS rent_charge_less_discount_extension_aed,

    IFNULL(extra_day_charge, 0) AS extra_day_charge, -- adjusted for early return
    IFNULL(delivery_charge, 0) AS delivery_charge,  -- adjusted for early return
    IFNULL(collection_charge, 0) AS collection_charge, -- adjusted for early return
    IFNULL(additional_driver_charge, 0) AS additional_driver_charge, -- adjusted for early return

    IFNULL(insurance_charge, 0) AS insurance_charge, -- adjusted for early 

    IFNULL(pai_charge, 0) AS pai_charge, -- adjusted for early 
    IFNULL(baby_seat, 0) AS baby_charge, -- adjusted for early 
    IFNULL(long_distance, 0) AS long_distance, -- adjusted for early
    IFNULL(premium_delivery, 0) AS premium_delivery, -- adjusted for early
    IFNULL(airport_delivery, 0) AS airport_delivery, -- adjusted for early
    IFNULL(gps_charge, 0) AS gps_charge, -- adjusted for early
    IFNULL(delivery_update, 0) AS delivery_update, -- adjusted for early

    IFNULL(intercity_charge, 0) AS intercity_charge, -- adjusted for early
    IFNULL(millage_charge, 0) AS millage_charge,
    IFNULL(other_rental_charge, 0) AS other_rental_charge, -- adjusted for early

    IFNULL(discount_charge, 0) AS discount_charge, -- adjusted for early
    IFNULL(discount_charge * tb.conversion_rate, 0) AS discount_charge_aed,
    IFNULL(discount_extension_charge, 0) AS discount_extension_charge,

    IFNULL(total_vat, 0) AS total_vat, -- adjusted for early
    IFNULL(other_charge, 0) AS other_charge, -- adjusted for early

    IFNULL(booking_charge, 0) AS booking_charge, -- adjusted for early
    IFNULL(booking_charge_less_discount, 0) AS booking_charge_less_discount, -- adjusted for early
    IFNULL(booking_charge * tb.conversion_rate, 0) AS booking_charge_aed,
    IFNULL(booking_charge_less_discount * tb.conversion_rate, 0) AS booking_charge_less_discount_aed,

    -- EXTENSION CALCS
    (booking_charge - (((customer_rate + insurance_rate + additional_driver_rate + pai_rate + baby_seat_rate) * extension_days) - discount_extension_charge)) AS booking_charge_less_extension,

    (booking_charge_less_discount - ((customer_rate + insurance_rate + additional_driver_rate + pai_rate + baby_seat_rate) * extension_days)) AS booking_charge_less_discount_extension,

    (booking_charge - (((customer_rate + insurance_rate + additional_driver_rate + pai_rate + baby_seat_rate) * extension_days) - discount_extension_charge)) * tb.conversion_rate AS booking_charge_less_extension_aed,

    (booking_charge_less_discount - ((customer_rate + insurance_rate + additional_driver_rate + pai_rate + baby_seat_rate) * extension_days)) * tb.conversion_rate AS booking_charge_less_discount_extension_aed,

    IFNULL(base_rental_revenue, 0) AS base_rental_revenue, -- adjusted for early
    IFNULL(non_rental_charge, 0) AS non_rental_charge, -- adjusted for early
    
    -- EXTENSION CHARGE CALC --
    (((customer_rate + insurance_rate + additional_driver_rate + pai_rate + baby_seat_rate) * extension_days) - discount_extension_charge) AS extension_charge,
    
    (((customer_rate + insurance_rate + additional_driver_rate + pai_rate + baby_seat_rate) * extension_days) - discount_extension_charge) * tb.conversion_rate AS extension_charge_aed,

    -- REVISED is_extension DEFINITION
    CASE
        WHEN extension_days >= 1 THEN "YES"
        ELSE "NO"
    END AS is_extended,

    Promo_Code AS promo_code,
    promo_code_discount_amount,
    -- IFNULL(IF(DATE_FORMAT(promocode_created_date, 
    --             '%Y-%m-%d %H:%i:%s') = '0000-00-00 00:00:00',
    --             '1900-01-01 12:00:00',
    --             promocode_created_date),
    --             '1900-01-01 12:00:00') AS promocode_created_date,
    CASE 
        WHEN DATE_FORMAT(promocode_created_date, '%Y-%m-%d %H:%i:%s') = '0000-00-00 00:00:00' THEN NULL
        WHEN promocode_created_date IS NULL THEN NULL
        ELSE DATE_FORMAT(promocode_created_date, '%Y-%m-%d %H:%i:%s')
    END AS promocode_created_date,
    REPLACE(promo_code_description, ',', '') AS promo_code_description,
    department AS promo_code_department,    

    -- Expiray_date AS promo_code_expiration_date, 
    CASE
        WHEN STR_TO_DATE(Expiray_date, '%m/%d/%Y') IS NOT NULL THEN Expiray_date
        WHEN Expiray_date = '31/12/2025' THEN '12/31/2025'
        WHEN STR_TO_DATE(Expiray_date, '%d/%m/%Y') IS NOT NULL THEN STR_TO_DATE(Expiray_date, '%d/%m/%Y')
        ELSE NULL -- or a default date value
    END AS promo_code_expiration_date,
    
    car_avail_id,
    car_cat_id,
    car_cat_name,
    REPLACE(requested_car, ',', '') AS requested_car,
    car_name,
    make,
    REPLACE(color, ',', '') AS color,

    deliver_country,
    deliver_city,
    country_id,
    city_id,
    REPLACE(REPLACE(REPLACE(delivery_location,
                '
                ',
                ''),
            ',',
            ''),
        '"',
        '') AS delivery_location,
    deliver_method,

    -- ADJUSTED CODE FOR UPLOAD TO BIGQUERY; CODE USED FOR COLLECTION BELOW DIDN'T WORK FOR DELIERY
    CASE
        WHEN delivery_lat IS NULL THEN ''
        WHEN delivery_lat = '' THEN ''
        ELSE REPLACE(delivery_lat, ',', '')
    END AS delivery_lat,
    CASE
        WHEN delivery_lng IS NULL THEN ''
        WHEN delivery_lng = '' THEN ''
        ELSE REPLACE(delivery_lng, ',', '')
    END AS delivery_lng,

    REPLACE(REPLACE(REPLACE(collection_location,
                '
                ',
                ''),
            ',',
            ''),
        '"',
        '') AS collection_location,
    collection_method,

    IFNULL(SUBSTRING_INDEX(collection_lat, ',', 1),
            collection_lat) AS collection_lat,
    IFNULL(SUBSTRING_INDEX(collection_lng, ',', 1),
            collection_lat) AS collection_lng,

    nps_score,
	NULLIF(REPLACE(REPLACE(REPLACE(nps_comment, '\n', ''), ',', ''), '"', ''), '') AS nps_comment
FROM
    (SELECT 
        b.id AS booking_id,
            agreement_number,
            b.millage_id,
            b.contract_id,
            b.drg,
            b.wrg,
            b.mrg,
            b.dr,
            b.wr,
            b.mr,
            b.deliver_date_string,
            
            DATE_FORMAT(DATE_ADD(b.created_on, INTERVAL 4 HOUR), '%Y-%m-%d %H:%i:%s') AS booking_datetime,
            DATE_FORMAT(DATE_ADD(b.created_on, INTERVAL 4 HOUR), '%Y') AS booking_year,
            QUARTER(DATE_ADD(b.created_on, INTERVAL 4 HOUR)) AS booking_quarter,
            DATE_FORMAT(DATE_ADD(b.created_on, INTERVAL 4 HOUR), '%m') AS booking_month,
            DATE_FORMAT(DATE_ADD(b.created_on, INTERVAL 4 HOUR), '%d') AS booking_day_of_month,
            WEEKOFYEAR(DATE_ADD(b.created_on, INTERVAL 4 HOUR)) AS booking_week_of_year,
            DAYOFWEEK(DATE_ADD(b.created_on, INTERVAL 4 HOUR)) AS booking_day_of_week,
            DATE_FORMAT(DATE_ADD(b.created_on, INTERVAL 4 HOUR), '%W') AS booking_day_of_week_v2,
            DATE_FORMAT(DATE_ADD(b.created_on, INTERVAL 4 HOUR), '%H') AS booking_time_bucket,
            
            (SELECT 
                    MAX(DATE_ADD(created_on, INTERVAL 4 HOUR))
                FROM
                    myproject.rental_car_booking2
            ) AS max_booking_datetime,
            
            DATE_FORMAT(CONCAT(STR_TO_DATE(b.deliver_date_string, '%d/%m/%Y'), ' ', b.deliver_time_string), '%Y-%m-%d %H:%i:%s') AS pickup_datetime,
            DATE_FORMAT(CONCAT(STR_TO_DATE(b.deliver_date_string, '%d/%m/%Y'), ' ', b.deliver_time_string), '%Y') pickup_year,
            QUARTER(STR_TO_DATE(b.deliver_date_string, '%d/%m/%Y')) AS pickup_quarter,
            DATE_FORMAT(CONCAT(STR_TO_DATE(b.deliver_date_string, '%d/%m/%Y'), ' ', b.deliver_time_string), '%m') pickup_month,
            DATE_FORMAT(CONCAT(STR_TO_DATE(b.deliver_date_string, '%d/%m/%Y'), ' ', b.deliver_time_string), '%d') pickup_day_of_month,
            WEEKOFYEAR(CONCAT(STR_TO_DATE(b.deliver_date_string, '%d/%m/%Y'), ' ', b.deliver_time_string)) AS pickup_week_of_year,
            DAYOFWEEK(CONCAT(STR_TO_DATE(b.deliver_date_string, '%d/%m/%Y'), ' ', b.deliver_time_string)) AS pickup_day_of_week,
            DATE_FORMAT(CONCAT(STR_TO_DATE(b.deliver_date_string, '%d/%m/%Y'), ' ', b.deliver_time_string), '%W') pickup_day_of_week_v2,
            DATE_FORMAT(CONCAT(STR_TO_DATE(b.deliver_date_string, '%d/%m/%Y'), ' ', b.deliver_time_string), '%H') pickup_time_bucket,
            
            -- RETURN DATE FIELDS = EITHER RETURN DATE OR EARLY RETURN DATE
            -- IF EARLY RETURN FLAG = 1 THEN USE NEW RETURN DATE FROM rental_early_return_bookings TABLE
            -- erb.new_return_date, -- MAX/Most recent early return date from rental_early_return_bookings
            -- erb.new_return_time, -- MAX/Most recent early return time
            b.early_return,
            CASE    
                WHEN b.early_return = 1 AND new_return_date THEN DATE_FORMAT(CONCAT(STR_TO_DATE(erb.new_return_date, '%d/%m/%Y'), ' ', erb.new_return_time), '%Y-%m-%d %H:%i:%s')
                ELSE DATE_FORMAT(CONCAT(STR_TO_DATE(b.return_date_string, '%d/%m/%Y'), ' ', b.return_time_string), '%Y-%m-%d %H:%i:%s')
            END AS return_datetime,

            (SELECT 
                    status
                FROM
                    myproject.rental_status rs
                WHERE
                    rs.id = b.status
                LIMIT 1) AS status,
                
            -- (CASE
            --     WHEN b.days < 7 THEN 'daily'
            --     WHEN b.days > 29 AND is_subscription = 1 THEN 'Subscription'
            --     WHEN b.days > 29 THEN 'Monthly'
            --     ELSE 'Weekly'
            -- END) AS booking_type,

            -- adjusted for early return
            CASE
                WHEN b.early_return = 0 THEN
                    CASE
                        WHEN b.days < 7 THEN 'daily'
                        WHEN b.days > 29 AND is_subscription = 1 THEN 'Subscription'
                        WHEN b.days > 29 THEN 'Monthly'
                        ELSE 'Weekly'
                    END
                ELSE 
                    CASE
                        WHEN erb.new_days < 7 THEN 'daily'
                        WHEN erb.new_days > 29 AND is_subscription = 1 THEN 'Subscription'
                        WHEN erb.new_days > 29 THEN 'Monthly'
                        ELSE 'Weekly'
                    END
            END AS booking_type,

            (CASE   
                WHEN b.vendor_id = 234555 THEN 'Dispatch'
                WHEN b.vendor_id <> 234555 THEN 'MarketPlace'
                ELSE 'N/A'
            END) AS marketplace_or_dispatch,
            (SELECT 
                    name
                FROM
                    myproject.rental_vendors rv
                WHERE
                    rv.owner_id = b.vendor_id
                        AND b.vendor_id <> 234555
                LIMIT 1) AS marketplace_partner,

            (SELECT 
                    name
                FROM
                    myproject.rental_vendors rv
                WHERE
                    rv.owner_id = b.vendor_id
                LIMIT 1) AS marketplace_partner_summary,

            b.platform_generated AS booking_channel,

            -- (SELECT 
            --         name
            --     FROM
            --         myproject.rental_car_booking_source bs
            --     WHERE
            --         bs.id = b.car_booking_source_id 
            --     LIMIT 1) AS booking_source,
            bs.name AS booking_source, 

            '' total_lifetime_booking_revenue,

            (CASE
                WHEN
                    (SELECT 
                            COUNT(1)
                        FROM
                            myproject.rental_car_booking2 bb
                        WHERE
                            bb.owner_id = b.owner_id) > 1 -- owner_id is customer_id
                THEN
                    'YES'
                ELSE 'NO'
            END) repeated_user,

            IFNULL((SELECT 
                    COUNT(1)
                FROM
                    myproject.rental_car_booking2 bb
                WHERE
                    bb.owner_id = b.owner_id), 0) AS no_of_bookings,

            IFNULL((SELECT 
                    COUNT(1)
                FROM
                    myproject.rental_car_booking2 bb
                WHERE
                    bb.owner_id = b.owner_id
                        AND bb.status = 8), 0) AS no_of_cancel_bookings,

            IFNULL((SELECT 
                    COUNT(1)
                FROM
                    myproject.rental_car_booking2 bb
                WHERE
                    bb.owner_id = b.owner_id
                        AND bb.status = 9), 0) AS no_of_completed_bookings,

            IFNULL((SELECT 
                    COUNT(1)
                FROM
                    myproject.rental_car_booking2 bb
                WHERE
                    bb.owner_id = b.owner_id 
                        AND bb.status NOT IN (8 , 9)), 0) AS no_of_started_bookings,
            b.owner_id AS customer_id,
            au.first_name AS first_name,
            au.last_name AS last_name,
            au.email as email,
            au.username as user_name,

            f.date_of_birth,

            TIMESTAMPDIFF(YEAR, STR_TO_DATE(f.date_of_birth, '%d/%m/%Y'), NOW()) age,   

            DATE_FORMAT(DATE_ADD(f.date_join, INTERVAL 4 HOUR), '%Y-%m-%d %H:%i:%s') AS date_join_gst,
            DATE_FORMAT(DATE_ADD(f.date_join, INTERVAL 4 HOUR), '%Y-%m-%d') AS date_join_formatted_gst,
            DATE_FORMAT(DATE_ADD(f.date_join, INTERVAL 4 HOUR), '%Y-%m') AS date_join_cohort,
            DATE_FORMAT(DATE_ADD(f.date_join, INTERVAL 4 HOUR), '%Y') AS date_join_year,
            DATE_FORMAT(DATE_ADD(f.date_join, INTERVAL 4 HOUR), '%m') AS date_join_month,

            -- when renter phone code = rental country phone code
            CASE
                WHEN f.is_resident = co.code THEN 'is_resident'
                WHEN f.is_resident <> co.code THEN 'is_non_resident'
                ELSE 'unknown'
            END AS resident_category,

            IFNULL((SELECT 
                    name
                FROM
                    myproject.rental_country ct
                WHERE
                    ct.code = dl_country
                LIMIT 1), 0) AS customer_driving_country,

            IFNULL((CASE
                WHEN f.is_verified > 0 THEN 'YES'
                ELSE 'NO'
            END), 0) AS customer_doc_vertification_status,

            -- adjusted for early return
            CASE
                WHEN b.early_return = 0 THEN IFNULL(b.days, 0)
                ELSE IFNULL(erb.new_days, b.days)
            END AS days,
            
            -- adjusted for early return
            CASE
                WHEN b.early_return = 0 THEN
                    IFNULL((
                        SELECT 
                            SUM(total_charge)
                        FROM
                            myproject.rental_charges cc
                        WHERE
                            cc.booking_id = b.id
                                AND cc.charge_type_id IN (30 , 31)), 0)
                ELSE 
                    IFNULL((
                        SELECT 
                            SUM(total_charge)
                        FROM
                            myproject.rental_early_return_charges as erc
                        WHERE
                            erc.booking_id = b.id
                                AND erc.charge_type_id IN (30 , 31)), 0)
            END AS extra_day_calc,

            -- adjusted for early return
            CASE
                WHEN b.early_return = 0 THEN
                    IFNULL(
                        myproject.get_rental_rates(
                            b.id,
                            b.millage_id,
                            b.contract_id,
                            b.drg,
                            b.wrg,
                            b.mrg,
                            b.dr,
                            b.wr,
                            b.mr,
                            b.days,
                            b.deliver_date_string
                        ), 0)
                ELSE 
                    CASE
                        -- Fetch the charge for early return
                        WHEN IFNULL((
                            SELECT 
                                charge 
                            FROM
                                myproject.rental_early_return_charges as erc
                            WHERE
                                erc.booking_id = b.id
                                    AND erc.charge_type_id IN (4)
                            LIMIT 1
                        ), 0) = 0 THEN
                    -- CASE
                    --     -- Fetch the charge for early return
                    --     WHEN (
                    --         SELECT 
                    --             charge 
                    --         FROM
                    --             myproject.rental_early_return_charges as erc
                    --         WHERE
                    --             erc.booking_id = b.id
                    --                 AND erc.charge_type_id IN (4)
                    --         LIMIT 1
                    --     ) = 0 THEN
                            -- Use the original WHEN clause value
                            IFNULL(
                                myproject.get_rental_rates(
                                    b.id,
                                    b.millage_id,
                                    b.contract_id,
                                    b.drg,
                                    b.wrg,
                                    b.mrg,
                                    b.dr,
                                    b.wr,
                                    b.mr,
                                    b.days,
                                    b.deliver_date_string
                                ), 0)
                        ELSE
                            -- Use the charge if it's not zero
                            IFNULL((
                                SELECT 
                                    charge 
                                FROM
                                    myproject.rental_early_return_charges as erc
                                WHERE
                                    erc.booking_id = b.id
                                        AND erc.charge_type_id IN (4)
                                LIMIT 1
                            ), 0)
                    END
            END AS customer_rate, -- in local currency

            -- adjusted for early return
            CASE
                WHEN b.early_return = 0 THEN
                    IFNULL((CASE
                        WHEN 
                            (SELECT 
                                SUM(total_charge)
                            FROM
                                myproject.rental_charges cc
                            WHERE
                                cc.booking_id = b.id
                                    AND cc.charge_type_id IN (15 , 36)) > 0 
                                    THEN (
                                        CASE 
                                            WHEN b.days < 7 THEN b.DIR
                                            WHEN b.days > 29 THEN b.MIR
                                            ELSE b.WIR
                                        END)
                        ELSE 0
                    END), 0)
                WHEN b.early_return = 1 THEN
                    IFNULL((CASE
                        WHEN 
                            (SELECT 
                                SUM(total_charge)
                            FROM
                                myproject.rental_early_return_charges as erc
                            WHERE
                                erc.booking_id = b.id
                                    AND erc.charge_type_id IN (15 , 36)) > 0 
                                    THEN (
                                        CASE 
                                            WHEN b.days < 7 THEN b.DIR
                                            WHEN b.days > 29 THEN b.MIR
                                            ELSE b.WIR
                                        END)
                        ELSE 0
                    END), 0)
                ELSE 0
            END AS insurance_rate, -- in local currency

            -- IFNULL((
            --     CASE
            --         WHEN
            --             (CASE
            --                 WHEN b.days < 7 THEN b.DIR
            --                 WHEN b.days > 29 THEN b.MIR
            --                 ELSE b.WIR
            --             END) > 0
            --         THEN
            --             'Full Insurance'
            --         ELSE ''
            --     END), 0) 
            -- AS insurance_type,

            -- adjusted for early return
            CASE
                WHEN b.early_return = 0 THEN
                    IFNULL((
                        CASE
                            WHEN
                                (CASE
                                    WHEN b.days < 7 THEN b.DIR
                                    WHEN b.days > 29 THEN b.MIR
                                    ELSE b.WIR
                                END) > 0
                            THEN
                                'Full Insurance'
                            ELSE ''
                        END), 0) 
                ELSE 
                    IFNULL((
                        CASE
                            WHEN
                                (CASE
                                    WHEN erb.new_days < 7 THEN b.DIR
                                    WHEN erb.new_days > 29 THEN b.MIR
                                    ELSE b.WIR
                                END) > 0
                            THEN
                                'Full Insurance'
                            ELSE ''
                        END), 0) 
            END AS insurance_type,

            IFNULL((
                SELECT 
                    ad.rate
                FROM
                    myproject.cars_available_detail ad
                WHERE
                    ad.car_available_id = b.car_available_id
                        AND ad.millage_id = b.millage_id
                        AND ad.month_id = b.contract_id
                LIMIT 1), 0) AS millage_rate,

           IFNULL((
                SELECT 
                    name
                FROM
                    myproject.Allowed_Millage am
                WHERE
                    am.id = b.millage_id
                LIMIT 1), 0) AS millage_cap_km,

            -- adjusted for early return
            CASE
                WHEN b.early_return = 0 THEN
                    IFNULL((
                        SELECT 
                            SUM(total_charge)
                        FROM
                            myproject.rental_charges cc
                        WHERE
                            cc.booking_id = b.id
                                AND cc.charge_type_id IN (4)), 0)
                ELSE 
                    IFNULL((
                        SELECT 
                            SUM(total_charge)
                        FROM
                            myproject.rental_early_return_charges as erc
                        WHERE
                            erc.booking_id = b.id
                                AND erc.charge_type_id IN (4)), 0)
            END AS rent_charge,

            -- adjusted for early return
            CASE
                WHEN b.early_return = 0 THEN
                    IFNULL((
                        SELECT 
                            SUM(total_charge)
                        FROM
                            myproject.rental_charges cc
                        WHERE
                            cc.booking_id = b.id
                                AND cc.charge_type_id IN (31 , 30)), 0)
                ELSE 
                    IFNULL((
                        SELECT 
                            SUM(total_charge)
                        FROM
                            myproject.rental_early_return_charges as erc
                        WHERE
                            erc.booking_id = b.id
                                AND erc.charge_type_id IN (31 , 30)), 0)
            END AS extra_day_charge,
            
            -- adjusted for early return
            CASE
                WHEN b.early_return = 0 THEN
                    IFNULL((
                        SELECT 
                            SUM(total_charge)
                        FROM
                            myproject.rental_charges cc
                        WHERE
                            cc.booking_id = b.id
                                AND cc.charge_type_id = 11), 0)
                ELSE 
                    IFNULL((
                        SELECT 
                            SUM(total_charge)
                        FROM
                            myproject.rental_early_return_charges as erc
                        WHERE
                            erc.booking_id = b.id
                                AND erc.charge_type_id = 11), 0)
            END AS delivery_charge,
            
            -- adjusted for early return
            CASE
                WHEN b.early_return = 0 THEN
                    IFNULL((
                        SELECT 
                            SUM(total_charge)
                        FROM
                            myproject.rental_charges cc
                        WHERE
                            cc.booking_id = b.id
                                AND cc.charge_type_id = 3), 0)
                ELSE 
                    IFNULL((
                        SELECT 
                            SUM(total_charge)
                        FROM
                            myproject.rental_early_return_charges as erc
                        WHERE
                            erc.booking_id = b.id
                                AND erc.charge_type_id = 3), 0)
            END AS collection_charge,
            
            -- adjusted for early return
            CASE
                WHEN b.early_return = 0 THEN
                    IFNULL((
                        SELECT 
                            SUM(total_charge)
                        FROM
                            myproject.rental_charges cc
                        WHERE
                            cc.booking_id = b.id
                                AND cc.charge_type_id IN (21 , 40)), 0)
                ELSE 
                    IFNULL((
                        SELECT 
                            -- SUM(total_charge) 
                            SUM(charge)
                        FROM
                            myproject.rental_early_return_charges as erc
                        WHERE
                            erc.booking_id = b.id
                                AND erc.charge_type_id IN (21 , 40)), 0)
            END AS additional_driver_charge,
            
            -- adjusted for early return
            CASE
                WHEN b.early_return = 0 THEN
                    IFNULL((
                        SELECT 
                            SUM(total_charge) / days
                        FROM
                            myproject.rental_charges cc
                        WHERE
                            cc.booking_id = b.id
                                AND cc.charge_type_id IN (21 , 40)), 0)
                ELSE 
                    IFNULL((
                        SELECT 
                            -- changed to charge because total_charge had some values over 100,000; see booking_id 21899
                            SUM(charge) / days 
                        FROM
                            myproject.rental_early_return_charges as erc
                        WHERE
                            erc.booking_id = b.id
                                AND erc.charge_type_id IN (21 , 40)), 0)
            END AS additional_driver_rate,
            
            -- adjusted for early return
            CASE
                WHEN b.early_return = 0 THEN
                    IFNULL((
                        SELECT 
                            SUM(total_charge)
                        FROM
                            myproject.rental_charges cc
                        WHERE
                            cc.booking_id = b.id
                                AND cc.charge_type_id IN (15 , 36)), 0)
                ELSE 
                    IFNULL((
                        SELECT 
                            SUM(charge)
                        FROM
                            myproject.rental_early_return_charges as erc
                        WHERE
                            erc.booking_id = b.id
                                AND erc.charge_type_id IN (15 , 36)), 0)
            END AS insurance_charge,
            
            -- adjusted for early return
            CASE
                WHEN b.early_return = 0 THEN
                    IFNULL((
                        SELECT 
                            SUM(total_charge)
                        FROM
                            myproject.rental_charges cc
                        WHERE
                            cc.booking_id = b.id
                                AND cc.charge_type_id IN (19, 41)), 0)
                ELSE 
                    IFNULL((
                        SELECT 
                            SUM(total_charge)
                        FROM
                            myproject.rental_early_return_charges as erc
                        WHERE
                            erc.booking_id = b.id
                                AND erc.charge_type_id IN (19, 41)), 0)
            END AS pai_charge,
            
            -- adjusted for early return
            CASE
                WHEN b.early_return = 0 THEN
                    IFNULL((
                        SELECT 
                            SUM(total_charge) / days
                        FROM
                            myproject.rental_charges cc
                        WHERE
                            cc.booking_id = b.id
                                AND cc.charge_type_id IN (19, 41)), 0)
                ELSE 
                    IFNULL((
                        SELECT 
                            SUM(total_charge) / days
                        FROM
                            myproject.rental_early_return_charges as erc
                        WHERE
                            erc.booking_id = b.id
                                AND erc.charge_type_id IN (19, 41)), 0)
            END AS pai_rate,
            
            -- adjusted for early return
            CASE
                WHEN b.early_return = 0 THEN
                    IFNULL((
                        SELECT 
                            SUM(total_charge)
                        FROM
                            myproject.rental_charges cc
                        WHERE
                            cc.booking_id = b.id
                                AND cc.charge_type_id IN (16)), 0)
                ELSE 
                    IFNULL((
                        SELECT 
                            SUM(total_charge)
                        FROM
                            myproject.rental_early_return_charges as erc
                        WHERE
                            erc.booking_id = b.id
                                AND erc.charge_type_id IN (16)), 0)
            END AS baby_seat,
            
            -- adjusted for early return
            CASE
                WHEN b.early_return = 0 THEN
                    IFNULL((
                        SELECT 
                            SUM(total_charge) / days
                        FROM
                            myproject.rental_charges cc
                        WHERE
                            cc.booking_id = b.id
                                AND cc.charge_type_id IN (16)), 0)
                ELSE 
                    IFNULL((
                        SELECT 
                            SUM(total_charge) / days
                        FROM
                            myproject.rental_early_return_charges as erc
                        WHERE
                            erc.booking_id = b.id
                                AND erc.charge_type_id IN (16)), 0)
            END AS baby_seat_rate,
            
            -- adjusted for early return
            CASE
                WHEN b.early_return = 0 THEN
                    IFNULL((
                        SELECT 
                            SUM(total_charge)
                        FROM
                            myproject.rental_charges cc
                        WHERE
                            cc.booking_id = b.id
                                AND cc.charge_type_id IN (32)), 0)
                ELSE 
                    IFNULL((
                        SELECT 
                            SUM(total_charge)
                        FROM
                            myproject.rental_early_return_charges as erc
                        WHERE
                            erc.booking_id = b.id
                                AND erc.charge_type_id IN (32)), 0)
            END AS long_distance,
            
            -- adjusted for early return
            CASE
                WHEN b.early_return = 0 THEN
                    IFNULL((
                        SELECT 
                            SUM(total_charge)
                        FROM
                            myproject.rental_charges cc
                        WHERE
                            cc.booking_id = b.id
                                AND cc.charge_type_id IN (56)), 0)
                ELSE 
                    IFNULL((
                        SELECT 
                            SUM(total_charge)
                        FROM
                            myproject.rental_early_return_charges as erc
                        WHERE
                            erc.booking_id = b.id
                                AND erc.charge_type_id IN (56)), 0)
            END AS premium_delivery,
            
            -- adjusted for early return
            CASE
                WHEN b.early_return = 0 THEN
                    IFNULL((
                        SELECT 
                            SUM(total_charge)
                        FROM
                            myproject.rental_charges cc
                        WHERE
                            cc.booking_id = b.id
                                AND cc.charge_type_id IN (29)), 0)
                ELSE 
                    IFNULL((
                        SELECT 
                            SUM(total_charge)
                        FROM
                            myproject.rental_early_return_charges as erc
                        WHERE
                            erc.booking_id = b.id
                                AND erc.charge_type_id IN (29)), 0)
            END AS airport_delivery,
            
            -- adjusted for early return
            CASE
                WHEN b.early_return = 0 THEN
                    IFNULL((
                        SELECT 
                            SUM(total_charge)
                        FROM
                            myproject.rental_charges cc
                        WHERE
                            cc.booking_id = b.id
                                AND cc.charge_type_id IN (17)), 0)
                ELSE 
                    IFNULL((
                        SELECT 
                            SUM(total_charge)
                        FROM
                            myproject.rental_early_return_charges as erc
                        WHERE
                            erc.booking_id = b.id
                                AND erc.charge_type_id IN (17)), 0)
            END AS gps_charge,
            
            -- adjusted for early return
            CASE
                WHEN b.early_return = 0 THEN
                    IFNULL((
                        SELECT 
                            SUM(total_charge)
                        FROM
                            myproject.rental_charges cc
                        WHERE
                            cc.booking_id = b.id
                                AND cc.charge_type_id IN (51)), 0)
                ELSE 
                    IFNULL((
                        SELECT 
                            SUM(total_charge)
                        FROM
                            myproject.rental_early_return_charges as erc
                        WHERE
                            erc.booking_id = b.id
                                AND erc.charge_type_id IN (51)), 0)
            END AS delivery_update,
            
            -- adjusted for early return
            CASE
                WHEN b.early_return = 0 THEN
                    IFNULL((
                        SELECT 
                            SUM(total_charge)
                        FROM
                            myproject.rental_charges cc
                        WHERE
                            cc.booking_id = b.id
                                AND cc.charge_type_id = 25), 0)
                ELSE 
                    IFNULL((
                        SELECT 
                            SUM(total_charge)
                        FROM
                            myproject.rental_early_return_charges as erc
                        WHERE
                            erc.booking_id = b.id
                                AND erc.charge_type_id = 25), 0)
            END AS intercity_charge,

            0 AS millage_charge,
            
            -- adjusted for early return
            CASE
                WHEN b.early_return = 0 THEN
                    IFNULL((
                        SELECT 
                            SUM(total_charge)
                        FROM
                            myproject.rental_charges cc
                        WHERE
                            cc.booking_id = b.id
                                -- AND cc.charge_type_id IN (18, 23, 26, 37, 38, 39, 48, 49, 50, 52, 57)), 0)
                                AND cc.charge_type_id IN (18, 23, 26, 35, 37, 38, 39, 48, 49, 50, 52, 57)), 0)
                ELSE 
                    IFNULL((
                        SELECT 
                            SUM(total_charge)
                        FROM
                            myproject.rental_early_return_charges as erc
                        WHERE
                            erc.booking_id = b.id
                                -- AND erc.charge_type_id IN (18, 23, 26, 37, 38, 39, 48, 49, 50, 52, 57)), 0)
                                AND erc.charge_type_id IN (18, 23, 26, 35, 37, 38, 39, 48, 49, 50, 52, 57)), 0)
            END AS other_rental_charge,

            -- (SELECT 
            --         SUM(total_charge)
            --     FROM
            --         myproject.rental_charges cc
            --     WHERE
            --         cc.booking_id = b.id
            --             AND cc.charge_type_id = 14) AS discount_charge,

            -- ROLLUP = RETURN THE TOTAL DISCOUNT
            -- adjusted for early return
            CASE
                WHEN b.early_return = 0 THEN
                    IFNULL((
                        SELECT 
                            SUM(cc.total_charge) AS total_discount
                        FROM
                            myproject.rental_charges cc
                        WHERE
                            cc.booking_id = b.id
                                AND cc.charge_type_id IN (14)), 0)
                ELSE 
                    IFNULL((
                        SELECT 
                            SUM(erc.total_charge) AS total_discount
                        FROM
                            myproject.rental_early_return_charges as erc
                        WHERE
                            erc.booking_id = b.id
                                AND erc.charge_type_id IN (14)), 0)
            END AS discount_charge, -- total discount charge
                    
            -- ROLLUP = RETURN ONLY THE EXTENSION DISCOUNT
            -- EXTENTION DISCOUNT ONLY (NOT PERFECT)
            -- BASICALLY LOOKS FOR A DISCOUNT CHARGE ID 14 THAT APPLIED AFTER THE BOOKING CREATED DATE
            -- ATTEMPTED TO USE THE COMMENTS WITH %EXTENSION% BUT WAS LESS ACCURATE DUE TO INCONSISTENT USE OF COMMENTS
            IFNULL((CASE
                -- WHEN is_extension THEN calc extension discount
                WHEN (SELECT 
                            CASE
                                WHEN COUNT(1) >= 1 THEN 'YES'
                                ELSE 'NO'
                            END
                        FROM
                            myproject.rental_invoice_details
                        WHERE
                            type = 'Extension' AND booking_id = b.id) = "YES" THEN (
                                SELECT
                                    SUM(rc.total_charge) AS extension_discount
                                    FROM
                                        myproject.rental_charges rc
                                        JOIN (
                                            SELECT
                                                booking_id,
                                                DATE_FORMAT(MIN(created_on), '%Y-%m-%d') as min_created_date
                                            FROM
                                                myproject.rental_charges
                                            GROUP BY
                                                booking_id
                                            ORDER BY
                                                booking_id
                                        ) AS min_date ON rc.booking_id = min_date.booking_id
                                    WHERE
                                        rc.booking_id = b.id
                                        AND rc.charge_type_id IN (14)
                                        -- if created on date for discount > min_date plus 1 day
                                        AND DATE_FORMAT(rc.created_on, '%Y-%m-%d') > DATE_FORMAT(DATE_ADD(min_date.min_created_date, INTERVAL 1 DAY), '%Y-%m-%d')
                                )
                ELSE 0
            END), 0) AS discount_extension_charge,
            
            -- adjusted for early return
            CASE
                WHEN b.early_return = 0 THEN
                    IFNULL((
                        SELECT 
                            SUM(total_charge)
                        FROM
                            myproject.rental_charges cc
                        WHERE
                            cc.booking_id = b.id
                                AND cc.charge_type_id = 20), 0)
                ELSE 
                    IFNULL((
                        SELECT 
                            SUM(total_charge)
                        FROM
                            myproject.rental_early_return_charges as erc
                        WHERE
                            erc.booking_id = b.id
                                AND erc.charge_type_id = 20), 0)
            END AS total_vat,
            
            -- adjusted for early return
            CASE
                WHEN b.early_return = 0 THEN
                    IFNULL((
                        SELECT 
                            SUM(total_charge)
                        FROM
                            myproject.rental_charges cc
                        WHERE
                            cc.booking_id = b.id
                                AND cc.charge_type_id IN (1 , 2, 8, 9, 13, 14, 20, 22, 24, 27, 28, 44, 45, 46, 47)), 0)
                ELSE 
                    IFNULL((
                        SELECT 
                            SUM(total_charge)
                        FROM
                            myproject.rental_early_return_charges as erc
                        WHERE
                            erc.booking_id = b.id
                                AND erc.charge_type_id IN (1 , 2, 8, 9, 13, 14, 20, 22, 24, 27, 28, 44, 45, 46, 47)), 0)
            END AS other_charge,
            
            -- adjusted for early return
            CASE
                WHEN b.early_return = 0 THEN
                    IFNULL((
                        SELECT 
                            SUM(total_charge)
                        FROM
                            myproject.rental_charges cc
                        WHERE
                            cc.booking_id = b.id
                                AND cc.charge_type_id IN (3, 4, 11, 15, 16, 17, 18, 19, 21, 23, 25, 26, 29, 30, 31, 32, 35, 36, 37, 38, 39, 40, 41, 48, 49, 50, 51, 52, 56, 57)), 0)
                -- fix  8/3/24
                ELSE 
                    CASE
                        -- Fetch the charge for early return
                        WHEN IFNULL((
                            SELECT 
                                charge 
                            FROM
                                myproject.rental_early_return_charges as erc
                            WHERE
                                erc.booking_id = b.id
                                    AND erc.charge_type_id IN (4)
                            LIMIT 1
                        ), 0) = 0 THEN
                            -- Use the original WHEN clause value
                            IFNULL(
                                myproject.get_rental_rates(
                                    b.id,
                                    b.millage_id,
                                    b.contract_id,
                                    b.drg,
                                    b.wrg,
                                    b.mrg,
                                    b.dr,
                                    b.wr,
                                    b.mr,
                                    b.days,
                                    b.deliver_date_string
                                ), 0)

                                -- adjusted for early return
                                * CASE
                                    WHEN b.early_return = 0 THEN IFNULL(b.days, 0)
                                    ELSE IFNULL(erb.new_days, b.days)
                                END

                                + IFNULL((
                                    SELECT 
                                        -- SUM(total_charge)
                                        SUM(CASE
                                                WHEN charge_type_id IN (21, 40) THEN (charge)
                                                WHEN charge_type_id IN (15, 36) THEN (charge)
                                                ELSE (total_charge)
                                            END)
                                    FROM
                                        myproject.rental_early_return_charges as erc
                                    WHERE
                                        erc.booking_id = b.id
                                            AND erc.charge_type_id IN (3, 4, 11, 15, 16, 17, 18, 19, 21, 23, 25, 26, 29, 30, 31, 32, 35, 36, 37, 38, 39, 40, 41, 48, 49, 50, 51, 52, 56, 57)), 0)
                        ELSE 
                            IFNULL((
                                SELECT 
                                    -- SUM(total_charge)
                                    SUM(CASE
                                            WHEN charge_type_id IN (21, 40) THEN (charge)
                                            WHEN charge_type_id IN (15, 36) THEN (charge)
                                            ELSE (total_charge)
                                        END)
                                FROM
                                    myproject.rental_early_return_charges as erc
                                WHERE
                                    erc.booking_id = b.id
                                        AND erc.charge_type_id IN (3, 4, 11, 15, 16, 17, 18, 19, 21, 23, 25, 26, 29, 30, 31, 32, 35, 36, 37, 38, 39, 40, 41, 48, 49, 50, 51, 52, 56, 57)), 0)
                    END
            END AS booking_charge,

            -- adjusted for early return
            CASE
                WHEN b.early_return = 0 THEN
                    IFNULL((
                        SELECT 
                            SUM(CASE
                                    WHEN charge_type_id IN (14) THEN -(total_charge)
                                    ELSE (total_charge)
                                END)
                        FROM
                            myproject.rental_charges cc
                        WHERE
                            cc.booking_id = b.id
                                AND cc.charge_type_id IN (3 , 4, 11, 15, 16, 17, 18, 19, 21, 23, 25, 26, 29, 30, 31, 32, 35, 36, 37, 38, 39, 40, 41, 48, 49, 50, 51, 52, 56, 57, 14)), 0)
                -- fix  8/3/24
                ELSE 
                    CASE
                        -- Fetch the charge for early return
                        WHEN IFNULL((
                            SELECT 
                                charge 
                            FROM
                                myproject.rental_early_return_charges as erc
                            WHERE
                                erc.booking_id = b.id
                                    AND erc.charge_type_id IN (4)
                            LIMIT 1
                        ), 0) = 0 THEN
                            -- Use the original WHEN clause value
                            IFNULL(
                                myproject.get_rental_rates(
                                    b.id,
                                    b.millage_id,
                                    b.contract_id,
                                    b.drg,
                                    b.wrg,
                                    b.mrg,
                                    b.dr,
                                    b.wr,
                                    b.mr,
                                    b.days,
                                    b.deliver_date_string
                                ), 0)

                                -- adjusted for early return
                                * CASE
                                    WHEN b.early_return = 0 THEN IFNULL(b.days, 0)
                                    ELSE IFNULL(erb.new_days, b.days)
                                END

                                + IFNULL((
                                    SELECT 
                                        SUM(CASE
                                                WHEN charge_type_id IN (14) THEN -(total_charge)
                                                WHEN charge_type_id IN (21, 40) THEN (charge)
                                                WHEN charge_type_id IN (15, 36) THEN (charge)
                                                ELSE (total_charge)
                                            END)
                                    FROM
                                        myproject.rental_early_return_charges as erc
                                    WHERE
                                        erc.booking_id = b.id
                                            AND erc.charge_type_id IN (3 , 4, 11, 15, 16, 17, 18, 19, 21, 23, 25, 26, 29, 30, 31, 32, 35, 36, 37, 38, 39, 40, 41, 48, 49, 50, 51, 52, 56, 57, 14)), 0)
                        ELSE 
                            IFNULL((
                                SELECT 
                                    SUM(CASE
                                            WHEN charge_type_id IN (14) THEN -(total_charge)
                                            WHEN charge_type_id IN (21, 40) THEN (charge)
                                            WHEN charge_type_id IN (15, 36) THEN (charge)
                                            ELSE (total_charge)
                                        END)
                                FROM
                                    myproject.rental_early_return_charges as erc
                                WHERE
                                    erc.booking_id = b.id
                                        AND erc.charge_type_id IN (3 , 4, 11, 15, 16, 17, 18, 19, 21, 23, 25, 26, 29, 30, 31, 32, 35, 36, 37, 38, 39, 40, 41, 48, 49, 50, 51, 52, 56, 57, 14)), 0)
                    END
            END AS booking_charge_less_discount,
            
            -- adjusted for early return
            CASE
                WHEN b.early_return = 0 THEN
                    IFNULL((
                        SELECT 
                            SUM(total_charge)
                        FROM
                            myproject.rental_charges cc
                        WHERE
                            cc.booking_id = b.id
                                AND cc.charge_type_id IN (3 , 4, 11, 15, 16, 17, 18, 19, 21, 23, 25, 26, 29, 30, 31, 32, 35, 36, 37, 38, 39, 40, 41, 48, 49, 50, 51, 52, 56, 57)), 0)
                -- fix  8/3/24
                ELSE 
                    CASE
                        -- Fetch the charge for early return
                        WHEN IFNULL((
                            SELECT 
                                charge 
                            FROM
                                myproject.rental_early_return_charges as erc
                            WHERE
                                erc.booking_id = b.id
                                    AND erc.charge_type_id IN (4)
                            LIMIT 1
                        ), 0) = 0 THEN
                            -- Use the original WHEN clause value
                            IFNULL(
                                myproject.get_rental_rates(
                                    b.id,
                                    b.millage_id,
                                    b.contract_id,
                                    b.drg,
                                    b.wrg,
                                    b.mrg,
                                    b.dr,
                                    b.wr,
                                    b.mr,
                                    b.days,
                                    b.deliver_date_string
                                ), 0)

                                -- adjusted for early return
                                * CASE
                                    WHEN b.early_return = 0 THEN IFNULL(b.days, 0)
                                    ELSE IFNULL(erb.new_days, b.days)
                                END

                                + IFNULL((
                                    SELECT 
                                        -- SUM(total_charge)
                                        SUM(CASE
                                                WHEN charge_type_id IN (21, 40) THEN (charge)
                                                WHEN charge_type_id IN (15, 36) THEN (charge)
                                                ELSE (total_charge)
                                            END)
                                    FROM
                                        myproject.rental_early_return_charges as erc
                                    WHERE
                                        erc.booking_id = b.id
                                            AND erc.charge_type_id IN (3 , 4, 11, 15, 16, 17, 18, 19, 21, 23, 25, 26, 29, 30, 31, 32, 35, 36, 37, 38, 39, 40, 41, 48, 49, 50, 51, 52, 56, 57)), 0)    
                        ELSE 
                            IFNULL((
                                SELECT 
                                    -- SUM(total_charge)
                                    SUM(CASE
                                            WHEN charge_type_id IN (21, 40) THEN (charge)
                                            WHEN charge_type_id IN (15, 36) THEN (charge)
                                            ELSE (total_charge)
                                        END)
                                FROM
                                    myproject.rental_early_return_charges as erc
                                WHERE
                                    erc.booking_id = b.id
                                        AND erc.charge_type_id IN (3 , 4, 11, 15, 16, 17, 18, 19, 21, 23, 25, 26, 29, 30, 31, 32, 35, 36, 37, 38, 39, 40, 41, 48, 49, 50, 51, 52, 56, 57)), 0)                        
                        END
            END AS base_rental_revenue,
            
            -- adjusted for early return
            CASE
                WHEN b.early_return = 0 THEN
                    IFNULL((
                        SELECT 
                            SUM(total_charge)
                        FROM
                            myproject.rental_charges cc
                        WHERE
                            cc.booking_id = b.id
                                AND cc.charge_type_id IN (1 , 2, 8, 9, 13, 14, 20, 22, 24, 27, 28, 44, 45, 46, 47)), 0)
                ELSE 
                    IFNULL((
                        SELECT 
                            SUM(total_charge)
                        FROM
                            myproject.rental_early_return_charges as erc
                        WHERE
                            erc.booking_id = b.id
                                AND erc.charge_type_id IN (1 , 2, 8, 9, 13, 14, 20, 22, 24, 27, 28, 44, 45, 46, 47)), 0)
            END AS non_rental_charge,

            -- adjusted for early return
            CASE
                WHEN b.early_return = 0 THEN
                    IFNULL((
                        SELECT 
                            SUM(extension_days)
                        FROM rental_messagesuser m
                        WHERE 
                            m.booking_id = b.id
                            AND (m.subject LIKE '%exten%' OR m.subject=CONCAT('Late Rental Return for Booking#', m.booking_id))
                            AND m.extension_days > 0
                            AND m.message LIKE '%Dear Partner%'), 0)
                ELSE
                    IFNULL(
                        (SELECT 
                                CASE 
                                    WHEN SUM(extension_days) <= TIMESTAMPDIFF(DAY, 
                                        DATE_FORMAT(CONCAT(STR_TO_DATE(erb.new_return_date, '%d/%m/%Y'), ' ', erb.new_return_time), '%Y-%m-%d %H:%i:%s'), 
                                        DATE_FORMAT(CONCAT(STR_TO_DATE(b.return_date_string, '%d/%m/%Y'), ' ', b.return_time_string), '%Y-%m-%d %H:%i:%s'))
                                    THEN 0
                                    ELSE SUM(extension_days) - TIMESTAMPDIFF(DAY, 
                                        DATE_FORMAT(CONCAT(STR_TO_DATE(erb.new_return_date, '%d/%m/%Y'), ' ', erb.new_return_time), '%Y-%m-%d %H:%i:%s'), 
                                        DATE_FORMAT(CONCAT(STR_TO_DATE(b.return_date_string, '%d/%m/%Y'), ' ', b.return_time_string), '%Y-%m-%d %H:%i:%s'))
                                END
                            FROM 
                                rental_messagesuser m
                            WHERE 
                                m.booking_id = b.id
                                AND (m.subject LIKE '%exten%' OR m.subject = CONCAT('Late Rental Return for Booking#', m.booking_id))
                                AND m.extension_days > 0
                                AND m.message LIKE '%Dear Partner%'), 0)
            END AS extension_days,

            pc.Promo_Code,
            '' promo_code_discount_amount,
            DATE_FORMAT(pc.date_created, '%Y-%m-%d %H:%i:%s') promocode_created_date,
            b.Promo_Code promo_code_description,
            pc.department,
            pc.Expiray_date,

			b.car_available_id car_avail_id,
            c.cat_id car_cat_id,
            cat.cat_name car_cat_name,

            ca.car_name requested_car,
            c.car_name,
            c.make,
            c.color,

            co.name deliver_country,
            rc.name deliver_city,
            b.delivery_location,
            co.id country_id,
            rc.id city_id,
            (CASE
                WHEN b.self_pickup_status = 1 THEN 'Self'
                ELSE 'Delivery'
            END) deliver_method,

            b.delivery_location_lat delivery_lat,
            b.delivery_location_lng delivery_lng,
            b.collection_location,
            (CASE
                WHEN b.self_return_status = 1 THEN 'Self'
                ELSE 'Collection'
            END) collection_method,
            b.return_location_lat collection_lat,
            b.return_location_lng collection_lng,

            ct.conversion_rate AS conversion_rate,

            ff.rate nps_score,
            ff.comments nps_comment
                
        FROM myproject.rental_car_booking2 b
        
            INNER JOIN myproject.rental_fuser f ON f.user_ptr_id = b.owner_id

            INNER JOIN myproject.rental_city rc ON rc.id = b.city_id
            INNER JOIN myproject.rental_country co ON co.id = rc.CountryID
            LEFT JOIN country_conversion_rate ct ON rc.CountryID = ct.country_id 
            
            -- LEFT JOIN rental_city ci ON ci.id = b.city_id 
            -- LEFT JOIN country_conversion_rate ct ON ci.CountryID = ct.country_id

            LEFT JOIN myproject.rental_vendors rv ON rv.owner_id = b.vendor_id
        
            LEFT JOIN myproject.rental_car c ON c.id = b.car_id
            LEFT JOIN myproject.rental_cars_available ca ON ca.id = b.car_available_id
            LEFT JOIN myproject.rental_cat cat ON ca.cat_id = cat.id
        
            LEFT JOIN myproject.rental_add_promo_codes pc ON pc.id = b.Promo_Code_id
            LEFT JOIN myproject.auth_user au ON au.id = b.owner_id

            -- LEFT JOIN (
            --     SELECT MAX(id),rate,comments,booking_id 
            --     FROM myproject.rental_rentalfeedback rf 
            --     GROUP BY booking_id
            -- ) ff ON ff.booking_id = b.id

            -- Optimized feedback join using correct aggregation
            LEFT JOIN (
                SELECT MAX(id) AS id, rate, comments, booking_id 
                FROM myproject.rental_rentalfeedback 
                GROUP BY booking_id, rate, comments
            ) ff ON ff.booking_id = b.id

            LEFT JOIN rental_car_booking_source bs ON bs.id = b.car_booking_source_id 

            -- NEW JOINS ADDED 05/27/24 FOR EARLY RETURNS
            LEFT JOIN rental_early_return_bookings AS erb ON erb.booking_id = b.id AND erb.is_active = 1 
            -- RETURNS MOST RECENT DATE RECORDS FOR EACH booking_id USING is_active flag 1

	-- FOR USE IN MYSQL WITH VARIABLES IN LINE 1
	-- WHERE 1 = 1
        -- AND b.id NOT IN (390491) -- has bad characters; removed 3/29/25
        -- AND DATE(DATE_ADD(b.created_on, INTERVAL 4 HOUR)) BETWEEN @str_date AND @end_date
        -- LOGIC TO EXCLUDE TEST BOOKINGS
        -- AND COALESCE(b.vendor_id,'') NOT IN (5, 33, 218, 23086) 
        -- AND (LOWER(au.first_name) NOT LIKE '%test%' 
        -- AND LOWER(au.last_name) NOT LIKE '%test%' 
        -- AND LOWER(au.username) NOT LIKE '%test%' 
        -- AND LOWER(au.email) NOT LIKE '%test%')

        -- LOGIC EXCLUDE TEST USERS FROM auth_user
        -- REVISED ABOVE TO BELOW ON 10/11/24
        -- AND COALESCE(b.vendor_id,'') NOT IN (5, 33, 218, 23086)    
        -- AND LOWER(au.first_name) NOT LIKE '%test%'
        -- AND LOWER(au.last_name) NOT LIKE '%test%'
        -- AND LOWER(au.username) NOT LIKE '%test%'
        -- AND LOWER(au.email) NOT LIKE '%test%'
        -- AND au.last_name NOT LIKE 'N'
        -- AND au.email NOT LIKE 'abc@gmail.com'
        -- AND LOWER(au.first_name) not LIKE '%ezhire%' 
        -- AND LOWER(au.last_name) not like '%ezhire%' 
        -- AND LOWER(au.email) not like '%ezhire%'

    -- TBD
    -- WHERE b.id IN ('51859', '75241', '271272')

    -- BASE FARE MISSING
    -- ADD BASE FARE CODE = 35 TO other_rental_charge TO INCLUDE THE BASE FARE
    -- WHERE b.id IN ('87679', '151091', '121185', '134561', '153496', '168712', '168710' ,'145038','173788','173787')

    -- CUSTOMER RATE = 0, EARLY RETURN = 1
    -- WHERE b.id IN ('208353')
	-- WHERE b.id IN ('43646', '67222', '72671', '104647', '94785', '206269', '208353', '228349', '226414', '237124')

    -- RENTAL STARTED STATUS BUT RETURN DATE PRIOR TO NOW?
	-- WHERE b.id IN ('281854')
    -- WHERE b.status = '31'
    -- HAVING return_datetime < NOW()

    -- WHERE b.id IN ('21899') -- additional driver rate & charge adjustment
    -- WHERE b.id IN ('21899', '36872', '121894', '86817', '68985') -- additional driver or insurance charge rate & charge adjustment
    -- WHERE b.id IN ('182520', '182582', '178575')
    -- WHERE b.id IN ('240709', '240727', '240755', '277097') -- adjusted early return extension days
    -- WHERE 
        -- b.id IN ('225443', '210299', '30174')
        -- b.id IN ('210299', '30174', '240667', '240709', '240727', '240755')
       --  b.id IN ('240709', '240727', '240755') -- extension_days and early return?
        -- date(date_add(b.created_on,interval 4 hour)) between '2024-01-01' and '2024-01-01' 

	-- FOR TESTING / AUDITING ******* START *********
    -- HAVING booking_charge_less_discount < 0
    -- HAVING additional_driver_charge > 0
    -- HAVING extension_days > 0 AND early_return > 0

    -- -- AND COALESCE(b.vendor_id,'') IN (33, 5 , 218, 23086) -- LOGIC TO EXCLUDE TEST BOOKINGS
    -- -- AND b.id = '240842'
    -- AND b.id IN ("240667", "246876", "240842", "246867") -- need to remove DATE in where above to return all ids
        
	-- WHERE date(date_add(b.created_on,interval 4 hour)) between '2024-01-01' and '2024-01-01' 
	-- AND pc.Promo_Code IS NOT NULL
	-- AND b.id = "218138"
    -- WHERE b.id IN ("246414", "240667")
    -- WHERE b.id IN ("264404", "240667", "257685")
    -- WHERE b.id IN ("247089")
    -- WHERE b.id IN ('251982')
    -- WHERE b.id IN ("240667")
    -- AND b.id IN ('244042','257685','240885', '241700', '241916')
    -- WHERE b.id IN ("240667", "246876", "240842", "246867", "248667")
    -- WHERE b.id IN ("260575", "199506", "200086", "237968") -- evaluate rental charge issues
    -- WHERE b.id IN ("269956", '252036') -- remove extra comma in delivery lat
	-- FOR TESTING / AUDITING ******* END *********
	
	-- FOR USE IN NODE / JAVASCRIPT AS SQL SET VARIABLES DON'T WORK ******* START *********
	WHERE 1 = 1
        AND b.id NOT IN (390491) -- has bad characters; removed 3/29/25
        AND date(date_add(b.created_on,interval 4 hour)) between 'startDateVariable' and 'endDateVariable'

        -- LOGIC TO EXCLUDE TEST BOOKINGS
        -- AND COALESCE(b.vendor_id,'') NOT IN (33, 5 , 218, 23086) 
        -- AND (LOWER(au.first_name) NOT LIKE '%test%' 
        -- AND LOWER(au.last_name) NOT LIKE '%test%' 
        -- AND LOWER(au.username) NOT LIKE '%test%' 
        -- AND LOWER(au.email) NOT LIKE '%test%')

        -- LOGIC EXCLUDE TEST USERS FROM auth_user
        -- REVISED ABOVE TO BELOW ON 10/11/24
        AND COALESCE(b.vendor_id,'') NOT IN (5, 33, 218, 23086)    
        AND LOWER(au.first_name) NOT LIKE '%test%'
        AND LOWER(au.last_name) NOT LIKE '%test%'
        AND LOWER(au.username) NOT LIKE '%test%'
        AND LOWER(au.email) NOT LIKE '%test%'
        AND au.last_name NOT LIKE 'N'
        AND au.email NOT LIKE 'abc@gmail.com'
        AND LOWER(au.first_name) not LIKE '%ezhire%' 
        AND LOWER(au.last_name) not like '%ezhire%' 
        AND LOWER(au.email) not like '%ezhire%'

	-- FOR USE IN NODE / JAVASCRIPT AS SQL VARIABLES DON'T WORK ******* END *********

    ORDER BY b.id
    -- LIMIT 10
    ) tb;
`;

module.exports = { queryBookingData };