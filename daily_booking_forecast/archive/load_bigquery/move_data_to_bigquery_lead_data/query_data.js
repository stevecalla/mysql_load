const leadQuery = `
    SELECT 
        -- use CONCAT to wrap fields in double quotes to avoid issues with comma parsing in CSV files
        
        DATE_FORMAT(created_on_pst_lm, '%Y-%m-%d') AS created_on_pst_lm, -- date '2024-02-12'
        booking_id_bm,
        CONCAT('"', lead_id_lm_list, '"') AS lead_id_lm_list,
        lead_id,

        CONCAT('"', created_on_pst_lm_list, '"') AS created_on_pst_lm_list,
        -- created_on_timestamp_pst_lm, -- date '2024-02-12 03:17:03'
        DATE_FORMAT(CONVERT_TZ(created_on_timestamp_pst_lm, '+00:00', '+05:00'), '%Y-%m-%d %H:%i:%s Asia/Karachi') as created_on_timestamp_pst_lm,

        CONCAT('"', created_on_utc_bm_list, '"') AS created_on_utc_bm_list,
        -- booking_created_on_utc_bm, -- date '2024-02-12 03:17:03'
        DATE_FORMAT(CONVERT_TZ(booking_created_on_utc_bm, '+00:00', '+00:00'), '%Y-%m-%d %H:%i:%s UTC') as booking_created_on_utc_bm,

        CONCAT('"', created_on_pst_bm_list, '"') AS created_on_pst_bm_list,
        -- booking_created_on_pst_bm, -- date '2024-02-12 03:17:03'
        DATE_FORMAT(CONVERT_TZ(booking_created_on_pst_bm, '+00:00', '+00:00'), '%Y-%m-%d %H:%i:%s Asia/Karachi') as booking_created_on_pst_bm,

        rental_status,
        lead_status_id,

        rental_status_desc,
        lead_status_desc,

        renting_in_country,
        renting_in_country_abb,
        CONCAT('"',  renting_in_country_list_lm, '"') AS  renting_in_country_list_lm,
        renting_in_country_lm,
        CONCAT('"', country_list_bm, '"') AS country_list_bm,
        country_bm,
        CONCAT('"', source_name_list_lm, '"') AS source_name_list_lm,
        source_name_lm,
        
        DATE_FORMAT(min_lead_created_on_pst, '%Y-%m-%d') AS min_lead_created_on_pst, -- date '2024-02-12'

        CONCAT('"', min_created_on_pst_list_cl, '"') AS min_created_on_pst_list_cl,
        -- min_created_on_pst_cl, -- date '2024-02-12 03:17:03'
        DATE_FORMAT(CONVERT_TZ(min_created_on_pst_cl, '+00:00', '+00:00'), '%Y-%m-%d %H:%i:%s Asia/Karachi') as min_created_on_pst_cl,

        response_time,
        response_time_bin,
        CONCAT('"', shift_list, '"') AS shift_list,
        shift,

        -- max_created_on_gst, -- date '2024-02-12 03:17:03'      
        DATE_FORMAT(CONVERT_TZ(max_created_on_gst, '+00:00', '+04:00'), '%Y-%m-%d %H:%i:%s Asia/Dubai') as max_created_on_gst,

        count_bookings,
        count_leads,

        -- created_on_timestamp_utc -- date '2024-02-12 03:17:03'
        DATE_FORMAT(CONVERT_TZ(created_on_timestamp_utc, '+00:00', '+00:00'), '%Y-%m-%d %H:%i:%s UTC') as created_on_timestamp_utc

    FROM ezhire_lead_response_data.lead_response_data
    -- LIMIT 200000;
`;

const leadMetricsQuery = `
    SELECT 
        *
    FROM ezhire_lead_response_data.lead_response_metrics_data
    -- LIMIT 200000;
`;

module.exports = {
    leadQuery,
    leadMetricsQuery,
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