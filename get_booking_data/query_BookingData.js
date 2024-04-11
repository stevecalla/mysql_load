const queryBookingData = `

-- SET @str_date = '2024-01-01',@end_date = '2024-01-01';

-- ********* START ************ CHANGE LOG
-- 04/08/24 - update extension definition to (extension days (ED) * customer rate) + (ED * insurance rate) + (ED * additional driver rate) less discount
-- add additional driver rate line 193 and @531
    -- decided to divide additonal driver charge by days
    -- given logic attempt resulted in reconcilation errors
-- adjusted insurance rate to be used in extension charge calc (insurance rate = 0 when insurance charge null/0)
-- added baby seat, GPS and other add ons
-- adjusted is_extended definition since original def didn't match extended days
-- 4/10/24 = added max created on field = line 323 & 36
-- 4/10/24 = added today yes / no field = line 38
-- ********* END *************** CHANGE LOG

SELECT 
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
        WHEN DATE_FORMAT(booking_datetime, '%Y-%m-%d') = DATE_FORMAT(max_booking_datetime, '%Y-%m-%d') THEN 'yes'
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

	-- RETURN DATE FIELDS
    IFNULL(IF(return_datetime = '0000-00-00 00:00:00',
                NULL,
                DATE_FORMAT(return_datetime, '%Y-%m-%d')),
            NULL) AS return_date,
    IFNULL(IF(return_datetime = '0000-00-00 00:00:00',
                NULL,
                DATE_FORMAT(return_datetime, '%Y-%m-%d %H:%i:%s')),
            NULL) AS return_datetime,
    return_year,
    return_quarter,
    return_month,
    return_day_of_month,
    return_week_of_year,
    return_day_of_week,
    return_day_of_week_v2,
    return_time_bucket,
    
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
    customer_driving_country,
    customer_doc_vertification_status,
    
    days,
    IFNULL(extension_days, 0) AS extension_days, -- ADDED

    IFNULL(extra_day_calc, 0) AS extra_day_calc,
    IFNULL(customer_rate, 0) AS customer_rate,
    IFNULL(insurance_rate, 0) AS insurance_rate,
    IFNULL(additional_driver_rate, 0) AS additional_driver_rate,
    IFNULL(pai_rate, 0) AS pai_rate,
    IFNULL(baby_seat_rate, 0) AS baby_seat_rate,

    IFNULL(insurance_type, 0) AS insurance_type,
    IFNULL(millage_rate, 0) AS millage_rate,
    IFNULL(millage_cap_km, 0) AS millage_cap_km,

    IFNULL(rent_charge, 0) AS rent_charge,

    IFNULL((((days - extension_days) * customer_rate) - (discount_charge)) * tb.conversion_rate, 0) AS rent_charge_less_discount_extension_aed,

    IFNULL(extra_day_charge, 0) AS extra_day_charge,
    IFNULL(delivery_charge, 0) AS delivery_charge,
    IFNULL(collection_charge, 0) AS collection_charge,
    IFNULL(additional_driver_charge, 0) AS additional_driver_charge,
    IFNULL(insurance_charge, 0) AS insurance_charge,

    IFNULL(pai_charge, 0) AS pai_charge,
    IFNULL(baby_seat, 0) AS baby_charge,
    IFNULL(long_distance, 0) AS long_distance,
    IFNULL(premium_delivery, 0) AS premium_delivery,
    IFNULL(airport_delivery, 0) AS airport_delivery,
    IFNULL(gps_charge, 0) AS gps_charge,
    IFNULL(delivery_update, 0) AS delivery_update,

    IFNULL(intercity_charge, 0) AS intercity_charge,
    IFNULL(millage_charge, 0) AS millage_charge,
    IFNULL(other_rental_charge, 0) AS other_rental_charge,

    IFNULL(discount_charge, 0) AS discount_charge,
    IFNULL(discount_extension_charge, 0) AS discount_extension_charge,

    IFNULL(total_vat, 0) AS total_vat,
    IFNULL(other_charge, 0) AS other_charge,

    IFNULL(booking_charge, 0) AS booking_charge,
    IFNULL(booking_charge_less_discount, 0) AS booking_charge_less_discount,
    IFNULL(booking_charge * tb.conversion_rate, 0) AS booking_charge_aed,
    IFNULL(booking_charge_less_discount * tb.conversion_rate, 0) AS booking_charge_less_discount_aed,

    -- EXTENSION CALCS
    (booking_charge - (((customer_rate + insurance_rate + additional_driver_rate + pai_rate + baby_seat_rate) * extension_days) - discount_extension_charge)) AS booking_charge_less_extension,

    (booking_charge_less_discount - ((customer_rate + insurance_rate + additional_driver_rate + pai_rate + baby_seat_rate) * extension_days)) AS booking_charge_less_discount_extension,

    (booking_charge - (((customer_rate + insurance_rate + additional_driver_rate + pai_rate + baby_seat_rate) * extension_days) - discount_extension_charge)) * tb.conversion_rate AS booking_charge_less_extension_aed,

    (booking_charge_less_discount - ((customer_rate + insurance_rate + additional_driver_rate + pai_rate + baby_seat_rate) * extension_days)) * tb.conversion_rate AS booking_charge_less_discount_extension_aed,

    IFNULL(base_rental_revenue, 0) AS base_rental_revenue,
    IFNULL(non_rental_charge, 0) AS non_rental_charge,
    
    -- EXTENSION CHARGE CALC --
    (((customer_rate + insurance_rate + additional_driver_rate + pai_rate + baby_seat_rate) * extension_days) - discount_extension_charge) AS extension_charge,
    (((customer_rate + insurance_rate + additional_driver_rate + pai_rate + baby_seat_rate) * extension_days) - discount_extension_charge) * tb.conversion_rate AS extension_charge_aed,

    CASE
        WHEN extension_days >= 1 THEN "YES"
        ELSE "NO"
    END AS is_extended,

    Promo_Code AS promo_code,
    promo_code_discount_amount,
    IFNULL(IF(DATE_FORMAT(promocode_created_date, 
                '%Y-%m-%d %H:%i:%s') = '0000-00-00 00:00:00',
                '1900-01-01 12:00:00',
                promocode_created_date),
                '1900-01-01 12:00:00') AS promocode_created_date,
    promo_code_description,
    
    car_avail_id,
    car_cat_id,
    car_cat_name,
    requested_car,
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
    
    IFNULL(delivery_lat, collection_lat) AS delivery_lat,
    IFNULL(delivery_lng, collection_lng) AS delivery_lng,

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
            DATE_FORMAT(DATE_ADD(b.created_on, INTERVAL 4 HOUR), '%Y') booking_year,
            QUARTER(DATE_ADD(b.created_on, INTERVAL 4 HOUR)) AS booking_quarter,
            DATE_FORMAT(DATE_ADD(b.created_on, INTERVAL 4 HOUR), '%m') booking_month,
            DATE_FORMAT(DATE_ADD(b.created_on, INTERVAL 4 HOUR), '%d') booking_day_of_month,
            WEEKOFYEAR(DATE_ADD(b.created_on, INTERVAL 4 HOUR)) AS booking_week_of_year,
            DAYOFWEEK(DATE_ADD(b.created_on, INTERVAL 4 HOUR)) AS booking_day_of_week,
            DATE_FORMAT(DATE_ADD(b.created_on, INTERVAL 4 HOUR), '%W') booking_day_of_week_v2,
            DATE_FORMAT(DATE_ADD(b.created_on, INTERVAL 4 HOUR), '%H') booking_time_bucket,
            
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
            
            DATE_FORMAT(CONCAT(STR_TO_DATE(b.return_date_string, '%d/%m/%Y'), ' ', b.return_time_string), '%Y-%m-%d %H:%i:%s') AS return_datetime,
            DATE_FORMAT(CONCAT(STR_TO_DATE(b.return_date_string, '%d/%m/%Y'), ' ', b.return_time_string), '%Y') return_year,
            QUARTER(STR_TO_DATE(b.return_date_string, '%d/%m/%Y')) AS return_quarter,
            DATE_FORMAT(CONCAT(STR_TO_DATE(b.return_date_string, '%d/%m/%Y'), ' ', b.return_time_string), '%m') return_month,
            DATE_FORMAT(CONCAT(STR_TO_DATE(b.return_date_string, '%d/%m/%Y'), ' ', b.return_time_string), '%d') return_day_of_month,
            WEEKOFYEAR(CONCAT(STR_TO_DATE(b.return_date_string, '%d/%m/%Y'), ' ', b.return_time_string)) AS return_week_of_year,
            DAYOFWEEK(CONCAT(STR_TO_DATE(b.return_date_string, '%d/%m/%Y'), ' ', b.return_time_string)) AS return_day_of_week,
            DATE_FORMAT(CONCAT(STR_TO_DATE(b.return_date_string, '%d/%m/%Y'), ' ', b.return_time_string), '%W') return_day_of_week_v2,
            DATE_FORMAT(CONCAT(STR_TO_DATE(b.return_date_string, '%d/%m/%Y'), ' ', b.return_time_string), '%H') return_time_bucket,

            (SELECT 
                    status
                FROM
                    myproject.rental_status rs
                WHERE
                    rs.id = b.status) AS status,
                    
            (CASE
                WHEN b.days < 7 THEN 'daily'
                WHEN b.days > 29 AND is_subscription = 1 THEN 'Subscription'
                WHEN b.days > 29 THEN 'Monthly'
                ELSE 'Weekly'
            END) AS booking_type,

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
                        AND b.vendor_id <> 234555) AS marketplace_partner,

            (SELECT 
                    name
                FROM
                    myproject.rental_vendors rv
                WHERE
                    rv.owner_id = b.vendor_id) AS marketplace_partner_summary,
            b.platform_generated AS booking_channel,

            (SELECT 
                    name
                FROM
                    myproject.rental_car_booking_source bs
                WHERE
                    bs.id = b.car_booking_source_id) AS booking_source,

            '' total_lifetime_booking_revenue,

            (CASE
                WHEN
                    (SELECT 
                            COUNT(1)
                        FROM
                            myproject.rental_car_booking2 bb
                        WHERE
                            bb.owner_id = b.owner_id) > 1
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

            IFNULL((SELECT 
                    name
                FROM
                    myproject.rental_country ct
                WHERE
                    ct.code = dl_country), 0) AS customer_driving_country,

            IFNULL((CASE
                WHEN f.is_verified > 0 THEN 'YES'
                ELSE 'NO'
            END), 0) AS customer_doc_vertification_status,

            IFNULL(b.days, 0) AS days,
            -- b.days,

            IFNULL((SELECT 
                    SUM(total_charge)
                FROM
                    myproject.rental_charges cc
                WHERE
                    cc.booking_id = b.id
                        AND cc.charge_type_id IN (30 , 31)), 0) AS extra_day_calc,

            IFNULL(myproject.get_rental_rates(b.id,
                    b.millage_id,
                    b.contract_id,
                    b.drg,
                    b.wrg,
                    b.mrg,
                    b.dr,
                    b.wr,
                    b.mr,
                    b.days,
                    b.deliver_date_string),
            0) AS customer_rate, -- in local currency

            -- (CASE
            --     WHEN b.days < 7 THEN b.DIR
            --     WHEN b.days > 29 THEN b.MIR
            --     ELSE b.WIR
            -- END) AS insurance_rate,

            IFNULL((CASE
                WHEN 
                    (SELECT 
                            SUM(total_charge)
                    FROM
                        myproject.rental_charges cc
                    WHERE
                        cc.booking_id = b.id
                            AND cc.charge_type_id IN (15 , 36)) > 0 THEN (CASE 
                                                                        WHEN b.days < 7 THEN b.DIR
                                                                        WHEN b.days > 29 THEN b.MIR
                                                                        ELSE b.WIR
                                                                    END)
                ELSE 0
            END), 0) AS insurance_rate,
            
            IFNULL((CASE
                WHEN
                    (CASE
                        WHEN b.days < 7 THEN b.DIR
                        WHEN b.days > 29 THEN b.MIR
                        ELSE b.WIR
                    END) > 0
                THEN
                    'Full Insurance'
                ELSE ''
            END), 0) AS insurance_type,

            IFNULL((SELECT 
                    ad.rate
                FROM
                    myproject.cars_available_detail ad
                WHERE
                    ad.car_available_id = b.car_available_id
                        AND ad.millage_id = b.millage_id
                        AND ad.month_id = b.contract_id), 0) AS millage_rate,
           IFNULL((SELECT 
                    name
                FROM
                    myproject.Allowed_Millage am
                WHERE
                    am.id = b.millage_id), 0) AS millage_cap_km,

            IFNULL((SELECT 
                    SUM(total_charge)
                FROM
                    myproject.rental_charges cc
                WHERE
                    cc.booking_id = b.id
                        AND cc.charge_type_id = 4), 0) AS rent_charge,

            IFNULL((SELECT 
                    SUM(total_charge)
                FROM
                    myproject.rental_charges cc
                WHERE
                    cc.booking_id = b.id
                        AND cc.charge_type_id IN (31 , 30)), 0) AS extra_day_charge,
            IFNULL((SELECT 
                    SUM(total_charge)
                FROM
                    myproject.rental_charges cc
                WHERE
                    cc.booking_id = b.id
                        AND cc.charge_type_id = 11), 0) AS delivery_charge,
            IFNULL((SELECT 
                    SUM(total_charge)
                FROM
                    myproject.rental_charges cc
                WHERE
                    cc.booking_id = b.id
                        AND cc.charge_type_id = 3), 0) AS collection_charge,
            IFNULL((SELECT 
                    SUM(total_charge)
                FROM
                    myproject.rental_charges cc
                WHERE
                    cc.booking_id = b.id
                        AND cc.charge_type_id IN (21 , 40)), 0) AS additional_driver_charge,

            IFNULL((SELECT 
                    SUM(total_charge) / days
                FROM
                    myproject.rental_charges cc
                WHERE
                    cc.booking_id = b.id
                        AND cc.charge_type_id IN (21 , 40)), 0) AS additional_driver_rate,

            IFNULL((SELECT 
                    SUM(total_charge)
                FROM
                    myproject.rental_charges cc
                WHERE
                    cc.booking_id = b.id
                        AND cc.charge_type_id IN (15 , 36)), 0) AS insurance_charge,

            IFNULL((SELECT 
                    SUM(total_charge)
                FROM
                    myproject.rental_charges cc
                WHERE
                    cc.booking_id = b.id
                        AND cc.charge_type_id IN (19, 41)), 0) AS pai_charge,

            IFNULL((SELECT 
                    SUM(total_charge) / days
                FROM
                    myproject.rental_charges cc
                WHERE
                    cc.booking_id = b.id
                        AND cc.charge_type_id IN (19, 41)), 0) AS pai_rate,

            IFNULL((SELECT 
                    SUM(total_charge)
                FROM
                    myproject.rental_charges cc
                WHERE
                    cc.booking_id = b.id
                        AND cc.charge_type_id IN (16)), 0) AS baby_seat,

            IFNULL((SELECT 
                    SUM(total_charge) / days
                FROM
                    myproject.rental_charges cc
                WHERE
                    cc.booking_id = b.id
                        AND cc.charge_type_id IN (16)), 0) AS baby_seat_rate,

            IFNULL((SELECT 
                    SUM(total_charge)
                FROM
                    myproject.rental_charges cc
                WHERE
                    cc.booking_id = b.id
                        AND cc.charge_type_id IN (32)), 0) AS long_distance,

            IFNULL((SELECT 
                    SUM(total_charge)
                FROM
                    myproject.rental_charges cc
                WHERE
                    cc.booking_id = b.id
                        AND cc.charge_type_id IN (56)), 0) AS premium_delivery,

            IFNULL((SELECT 
                    SUM(total_charge)
                FROM
                    myproject.rental_charges cc
                WHERE
                    cc.booking_id = b.id
                        AND cc.charge_type_id IN (29)), 0) AS airport_delivery,

            IFNULL((SELECT 
                    SUM(total_charge)
                FROM
                    myproject.rental_charges cc
                WHERE
                    cc.booking_id = b.id
                        AND cc.charge_type_id IN (17)), 0) AS gps_charge,

            IFNULL((SELECT 
                    SUM(total_charge)
                FROM
                    myproject.rental_charges cc
                WHERE
                    cc.booking_id = b.id
                        AND cc.charge_type_id IN (51)), 0) AS delivery_update,

            IFNULL((SELECT 
                    SUM(total_charge)
                FROM
                    myproject.rental_charges cc
                WHERE
                    cc.booking_id = b.id
                        AND cc.charge_type_id = 25), 0) AS intercity_charge,
            0 AS millage_charge,
            IFNULL((SELECT 
                    SUM(total_charge)
                FROM
                    myproject.rental_charges cc
                WHERE
                    cc.booking_id = b.id
                        AND cc.charge_type_id IN (15, 18, 23, 26, 37, 38, 39, 48, 49, 50, 51, 52, 57)), 0) AS other_rental_charge,
            -- (SELECT 
            --         SUM(total_charge)
            --     FROM
            --         myproject.rental_charges cc
            --     WHERE
            --         cc.booking_id = b.id
            --             AND cc.charge_type_id = 14) AS discount_charge,

            -- ROLLUP = RETURN THE TOTAL DISCOUNT
            IFNULL((SELECT
                    SUM(rc.total_charge) AS total_discount
                FROM
                    myproject.rental_charges rc
                WHERE
                    rc.booking_id = b.id
                    AND rc.charge_type_id IN (14)), 0) AS discount_charge, -- total discount charge
                    
            -- ROLLUP = RETURN ONLY THE EXTENSION DISCOUNT
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

            IFNULL((SELECT 
                    SUM(total_charge)
                FROM
                    myproject.rental_charges cc
                WHERE
                    cc.booking_id = b.id
                        AND cc.charge_type_id = 20), 0) AS total_vat,
            IFNULL((SELECT 
                    SUM(total_charge)
                FROM
                    myproject.rental_charges cc
                WHERE
                    cc.booking_id = b.id
                        AND cc.charge_type_id IN (1 , 2, 8, 9, 13, 14, 20, 22, 24, 27, 28, 44, 45, 46, 47)), 0) AS other_charge,
            IFNULL((SELECT 
                    SUM(total_charge)
                FROM
                    myproject.rental_charges cc
                WHERE
                    cc.booking_id = b.id
                        AND cc.charge_type_id IN (3 , 4, 11, 15, 16, 17, 18, 19, 21, 23, 25, 26, 29, 30, 31, 32, 36, 37, 38, 39, 40, 41, 48, 49, 50, 51, 52, 56, 57)), 0) AS booking_charge,

            IFNULL((SELECT 
                    SUM(CASE
                            WHEN charge_type_id IN (14) THEN - (total_charge)
                            ELSE (total_charge)
                        END)
                FROM
                    myproject.rental_charges cc
                WHERE
                    cc.booking_id = b.id
                        AND cc.charge_type_id IN (3 , 4, 11, 15, 16, 17, 18, 19, 21, 23, 25, 26, 29, 30, 31, 32, 36, 37, 38, 39, 40, 41, 48, 49, 50, 51, 52, 56, 57, 14)), 0) AS booking_charge_less_discount,

            IFNULL((SELECT 
                    SUM(total_charge)
                FROM
                    myproject.rental_charges cc
                WHERE
                    cc.booking_id = b.id
                        AND cc.charge_type_id IN (3 , 4, 11, 15, 16, 17, 18, 19, 21, 23, 25, 26, 29, 30, 31, 32, 36, 37, 38, 39, 40, 41, 48, 49, 50, 51, 52, 56, 57)), 0) AS base_rental_revenue,

            IFNULL((SELECT 
                    SUM(total_charge)
                FROM
                    myproject.rental_charges cc
                WHERE
                    cc.booking_id = b.id
                        AND cc.charge_type_id IN (1 , 2, 8, 9, 13, 14, 20, 22, 24, 27, 28, 44, 45, 46, 47)), 0) AS non_rental_charge,

            IFNULL((SELECT 
                SUM(extension_days)
                FROM rental_messagesuser m
                WHERE 
                    m.booking_id = b.id
                    AND (m.subject LIKE '%exten%' OR m.subject=CONCAT('Late Rental Return for Booking#', m.booking_id))
                    AND m.extension_days > 0
                    AND m.message LIKE '%Dear Partner%'), 0)
                AS extension_days,

            pc.Promo_Code,
            '' promo_code_discount_amount,
            DATE_FORMAT(pc.date_created, '%Y-%m-%d %H:%i:%s') promocode_created_date,
            b.Promo_Code promo_code_description,

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

            (SELECT 
                    ct.conversion_rate
                FROM
                    myproject.country_conversion_rate ct, myproject.rental_city c
                WHERE
                    ct.country_id = c.CountryID
                        AND c.id = b.city_id) AS conversion_rate,

            (SELECT 
                    rate
                FROM
                    myproject.rental_rentalfeedback rf
                WHERE
                    rf.booking_id = b.id
                ORDER BY rf.id DESC
                LIMIT 1) nps_score,

            (SELECT 
                    comments
                FROM
                    myproject.rental_rentalfeedback rf
                WHERE
                    rf.booking_id = b.id
                ORDER BY rf.id DESC
                LIMIT 1) nps_comment
                
    FROM myproject.rental_car_booking2 b
    
    INNER JOIN myproject.rental_fuser f ON f.user_ptr_id = b.owner_id
    INNER JOIN myproject.rental_city rc ON rc.id = b.city_id
    INNER JOIN myproject.rental_country co ON co.id = rc.CountryID
    LEFT JOIN myproject.rental_vendors rv ON rv.owner_id = b.vendor_id
    
    LEFT JOIN myproject.rental_car c ON c.id = b.car_id
    LEFT JOIN myproject.rental_cars_available ca ON ca.id = b.car_available_id
    LEFT JOIN myproject.rental_cat cat ON ca.cat_id = cat.id -- ADDITION

    -- LEFT JOIN (SELECT id, car_name,cat_id FROM rental_cars_available) AS car ON r.car_available_id = car.id
    -- left join ( select id,cat_name from rental_cat) as cat on car.cat_id = cat.id
    
    LEFT JOIN myproject.rental_add_promo_codes pc ON pc.id = b.Promo_Code_id
    LEFT JOIN myproject.auth_user au ON au.id = b.owner_id

	-- FOR USE IN MYSQL WITH VARIABLES IN LINE 1
	-- WHERE 
    --     DATE(DATE_ADD(b.created_on, INTERVAL 4 HOUR)) BETWEEN @str_date AND @end_date
	-- 	AND COALESCE(b.vendor_id,'') NOT IN (33, 5 , 218, 23086) -- LOGIC TO EXCLUDE TEST BOOKINGS
	-- 	AND (LOWER(au.first_name) NOT LIKE '%test%' AND LOWER(au.last_name) NOT LIKE '%test%' AND LOWER(au.username) NOT LIKE '%test%' AND LOWER(au.email) NOT LIKE '%test%')
    
	-- FOR TESTING / AUDITING ******* START *********
    -- HAVING booking_charge_less_discount < 0
    -- HAVING additional_driver_charge > 0
    -- HAVING extension_days > 0

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
	-- FOR TESTING / AUDITING ******* END *********
	
	-- FOR USE IN NODE / JAVASCRIPT AS SQL SET VARIABLES DON'T WORK ******* START *********
	WHERE date(date_add(b.created_on,interval 4 hour)) between 'startDateVariable' and 'endDateVariable'
        AND COALESCE(b.vendor_id,'') NOT IN (33, 5 , 218, 23086) -- LOGIC TO EXCLUDE TEST BOOKINGS
		AND (LOWER(au.first_name) NOT LIKE '%test%' AND LOWER(au.last_name) NOT LIKE '%test%' AND LOWER(au.username) NOT LIKE '%test%' AND LOWER(au.email) NOT LIKE '%test%')
	-- FOR USE IN NODE / JAVASCRIPT AS SQL VARIABLES DON'T WORK ******* END *********

    ORDER BY b.id
    -- LIMIT 10
    ) tb;
`;

module.exports = { queryBookingData };