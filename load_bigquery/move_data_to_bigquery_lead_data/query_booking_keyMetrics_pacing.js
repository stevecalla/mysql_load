const exampleQuery = `
    SELECT 
        DATE_FORMAT(calendar_date, '%Y-%m-%d') AS calendar_date,
        
        calendar_year_month,calendar_year,calendar_quarter,calendar_month,calendar_week,
        calendar_day,
        
        DATE_FORMAT(CONVERT_TZ(max_booking_datetime, '+00:00', '-04:00'), '%Y-%m-%d %H:%i:%s UTC') as max_booking_datetime,
        
        cohort_join_year_month,cohort_join_year,cohort_join_quarter,cohort_join_month,
        cohort_month_start,calendar_month_start,months_diff,is_today,days_on_rent_whole_day,days_on_rent_fraction,
        trans_on_rent_count,booking_count,pickup_count,return_count,day_in_initial_period,day_in_extension_period,
        booking_charge_aed_rev_allocation,booking_charge_less_discount_aed_rev_allocation,rev_aed_in_initial_period,
        rev_aed_in_extension_period,vendor_on_rent_dispatch,vendor_on_rent_marketplace,booking_type_on_rent_daily,
        booking_type_on_rent_monthly,booking_type_on_rent_subscription,booking_type_on_rent_weekly,is_repeat_on_rent_no,
        is_repeat_on_rent_yes,country_on_rent_bahrain,country_on_rent_georgia,country_on_rent_kuwait,
        country_on_rent_oman,country_on_rent_pakistan,country_on_rent_qatar,country_on_rent_saudia_arabia,
        country_on_rent_serbia,country_on_rent_united_arab_emirates,
        
        DATE_FORMAT(CONVERT_TZ(created_at, '+00:00', '+07:00'), '%Y-%m-%d %H:%i:%s UTC') as created_at

    FROM ezhire_user_data.user_data_cohort_stats 
    -- LIMIT 200000;
`;

const leadQuery = `
    SELECT 
        created_on_pst_lm, -- date '2024-02-12'
        booking_id_bm,
        lead_id_lm_list,
        lead_id,
        created_on_pst_lm_list, -- string
        created_on_timestamp_pst_lm, -- date '2024-02-12 03:17:03'
        created_on_utc_bm_list, -- string list of dates
        booking_created_on_utc_bm, -- date '2024-02-12 03:17:03'
        created_on_pst_bm_list, -- string list of dates
        booking_created_on_pst_bm, -- date '2024-02-12 03:17:03'
        rental_status,
        lead_status_id,
        renting_in_country,
        renting_in_country_abb,
        renting_in_country_list_lm,
        renting_in_country_lm,
        country_list_bm,
        country_bm,
        source_name_list_lm,
        source_name_lm,
        min_lead_created_on_pst, -- date '2024-02-12'
        min_created_on_pst_list_cl, -- string list of dates
        min_created_on_pst_cl, -- date '2024-02-12 03:17:03'
        response_time,
        response_time_bin,
        shift_list,
        shift,
        max_created_on_gst, -- date '2024-02-12 03:17:03'
        count_bookings,
        count_leads,
        created_on_timestamp_utc -- date '2024-02-12 03:17:03'
    FROM ezhire_lead_response_data.lead_response_data
    -- LIMIT 200000;
`;

module.exports = {
    leadQuery
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

// DATE_FORMAT(STR_TO_DATE(date_of_birth, '%m/%d/%Y'), '%Y-%m-%d') AS date_of_birth
    // converts string '01/1/1995' to '1995-01-01' 


    