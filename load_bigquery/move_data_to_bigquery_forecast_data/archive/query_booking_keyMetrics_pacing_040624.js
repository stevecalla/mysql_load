const bookingQuery = `
    SELECT
        booking_id,
        agreement_number,

        DATE_FORMAT(booking_date, '%Y-%m-%d') AS booking_date,
        DATE_FORMAT(CONVERT_TZ(booking_datetime, '+00:00', '-04:00'), '%Y-%m-%d %H:%i:%s UTC') as booking_datetime,

        booking_year,booking_month,booking_day_of_month,booking_week_of_year,booking_day_of_week,booking_day_of_week_v2,booking_time_bucket,booking_count,booking_count_excluding_cancel,

        DATE_FORMAT(pickup_date, '%Y-%m-%d') AS pickup_date,
        DATE_FORMAT(CONVERT_TZ(pickup_datetime, '+00:00', '-04:00'), '%Y-%m-%d %H:%i:%s UTC') as pickup_datetime,

        pickup_month,pickup_day_of_month,pickup_week_of_year,pickup_day_of_week,pickup_day_of_week_v2,pickup_time_bucket,

        DATE_FORMAT(return_date, '%Y-%m-%d') AS return_date,
        DATE_FORMAT(CONVERT_TZ(return_datetime, '+00:00', '-04:00'), '%Y-%m-%d %H:%i:%s UTC') as return_datetime,
        
        return_month,return_day_of_month,return_week_of_year,return_day_of_week,return_day_of_week_v2,return_time_bucket,advance_category_day,advance_category_week,advance_category_month,advance_category_date_within_week,advance_pickup_booking_date_diff,comparison_28_days,comparison_period,comparison_common_date,Current_28_Days,4_Weeks_Prior,52_Weeks_Prior,status,booking_type,marketplace_or_dispatch,marketplace_partner,marketplace_partner_summary,booking_channel,booking_source,repeated_user,total_lifetime_booking_revenue,no_of_bookings,no_of_cancel_bookings,no_of_completed_bookings,no_of_started_bookings,customer_id,first_name,last_name,email,date_of_birth,age,customer_driving_country,customer_doc_vertification_status,days,extension_days,extra_day_calc,customer_rate,insurance_rate,insurance_type,millage_rate,millage_cap_km,rent_charge,rent_charge_less_discount_extension_aed,extra_day_charge,delivery_charge,collection_charge,additional_driver_charge,insurance_charge,intercity_charge,millage_charge,other_rental_charge,discount_charge,total_vat,other_charge,booking_charge,booking_charge_less_discount,booking_charge_aed,booking_charge_less_discount_aed,booking_charge_less_extension,booking_charge_less_discount_extension,booking_charge_less_extension_aed,booking_charge_less_discount_extension_aed,base_rental_revenue,non_rental_charge,extension_charge,extension_charge_aed,is_extended,promo_code,promo_code_discount_amount,promocode_created_date,promo_code_description,car_avail_id,car_cat_id,car_cat_name,requested_car,car_name,make,color,deliver_country,deliver_city,country_id,city_id,delivery_location,deliver_method,delivery_lat,delivery_lng,collection_location,collection_method,collection_lat,collection_lng,nps_score,nps_comment,

        -- DATE_FORMAT(created_at, '%Y-%m-%d %H:%i:%s UTC') as created_at
        DATE_FORMAT(CONVERT_TZ(created_at, '+00:00', '+07:00'), '%Y-%m-%d %H:%i:%s UTC') as created_at

    FROM ezhire_booking_data.booking_data 
    WHERE pickup_year IN (2023, 2024, 2025)

    -- WHERE pickup_year IN ('2024')
    -- WHERE booking_date IN ('2024-01-01')
    -- WHERE status NOT LIKE '%Cancel%'
    -- AND pickup_year IN (2023, 2024, 2025)

    ORDER BY booking_date ASC, pickup_date ASC
    -- LIMIT 1;
`;

const keyMetricsQuery = `
    SELECT
        DATE_FORMAT(CONVERT_TZ(created_at, '+00:00', '+00:00'), '%Y-%m-%d %H:%i:%s MST') as created_at,
        DATE_FORMAT(STR_TO_DATE(calendar_date, '%Y-%m-%d'), '%Y-%m-%d') AS calendar_date,

        year,quarter,month,week,day,days_on_rent_whole_day,days_on_rent_fraction,trans_on_rent_count,booking_count,pickup_count,return_count,day_in_initial_period,day_in_extension_period,booking_charge_aed_rev_allocation,booking_charge_less_discount_aed_rev_allocation,rev_aed_in_initial_period,rev_aed_in_extension_period,vendor_on_rent_dispatch,vendor_on_rent_marketplace,booking_type_on_rent_daily,booking_type_on_rent_monthly,booking_type_on_rent_subscription,booking_type_on_rent_weekly,is_repeat_on_rent_no,is_repeat_on_rent_yes,country_on_rent_bahrain,country_on_rent_georgia,country_on_rent_kuwait,country_on_rent_oman,country_on_rent_pakistan,country_on_rent_qatar,country_on_rent_saudia_arabia,country_on_rent_serbia,country_on_rent_united_arab_emirates
    FROM ezhire_key_metrics.key_metrics_data
    ORDER BY calendar_date ASC
    -- LIMIT 5;
`;

const pacingQuery = `
    SELECT
        pickup_month_year,

        DATE_FORMAT(booking_date, '%Y-%m-%d') AS booking_date,

        days_from_first_day_of_month,
        count,
        total_booking_charge_aed,
        total_booking_charge_less_discount_aed,
        total_booking_charge_less_discount_extension_aed,
        total_extension_charge_aed,
        running_count,running_total_booking_charge_aed,
        running_total_booking_charge_less_discount_aed,
        running_total_booking_charge_less_discount_extension_aed,
        running_total_extension_charge_aed,

        DATE_FORMAT(CONVERT_TZ(created_at, '+00:00', '+00:00'), '%Y-%m-%d %H:%i:%s MST') as created_at

    FROM ezhire_pacing_metrics.pacing_final_data
    ORDER BY pickup_month_year ASC, booking_date ASC
    -- LIMIT 5;
`;

module.exports = {
    bookingQuery,
    keyMetricsQuery,
    pacingQuery,
}

//NOTE: Need to fix date formats so biqquery would convert properly; BQ saves all timestamps in UTC thus need to convert for analysis/Looker queries 
//NOTE: source: https://www.googlecloudcommunity.com/gc/Data-Analytics/Timezone-in-BigQuery/m-p/698624
//NOTE: source: https://en.wikipedia.org/wiki/List_of_tz_database_time_zones

// DATE_FORMAT(booking_date, '%Y-%m-%d') AS booking_date
// converts 2022-11-08T07:00:00.000Z to '2022-11-08'

// DATE_FORMAT(CONVERT_TZ(booking_datetime, '+00:00', '+00:00'), '%Y-%m-%d %H:%i:%s Asia/Dubai') as booking_datetime,
    // converts 2024-01-01T07:18:36.000Z to '2024-01-01 00:18:36 Asia/Dubai'
    // then biquery will convert GST to UTC (less 4 hours)

// DATE_FORMAT(created_at, '%Y-%m-%d %H:%i:%s MST') as created_at
    // converts timestamp 2024-04-03T20:38:20.000Z to the format 2018-07-05 12:54:00 MST
    // then biquery will convert MST to UTC (or +7 hours)

// DATE_FORMAT(STR_TO_DATE('2023-01-01', '%Y-%m-%d'), '%Y-%m-%d') AS calendar_date,
    // converts string '2023-01-01' to date