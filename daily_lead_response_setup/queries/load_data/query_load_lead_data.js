const derived_fields = `
    created_on_pst_lm,
    
    booking_id_bm,

    lead_id_lm_list,
    lead_id,

    created_on_pst_lm_list,
    created_on_timestamp_pst_lm,

    created_on_utc_bm_list,

    booking_created_on_utc_bm,

    created_on_pst_bm_list,
    booking_created_on_pst_bm,

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

    min_lead_created_on_pst,
    min_created_on_pst_list_cl,
    min_created_on_pst_cl,

    response_time,
    response_time_bin,

    shift_list,
    shift,

    max_created_on_gst,

    count_bookings,
    count_leads,
    
    created_on_timestamp_utc
`;

const transform_fields = `
  -- HANDLE CREATED_ON_TIMESTAMP IN UTC
  created_on_timestamp_utc = UTC_TIMESTAMP()
`;

function query_load_lead_data(filePath, table) {
  return `
    LOAD DATA LOCAL INFILE '${filePath}'
    INTO TABLE ${table}
    FIELDS TERMINATED BY ','
    ENCLOSED BY '"'
    LINES TERMINATED BY '\\n'
    IGNORE 1 LINES
    (
      ${derived_fields} 
    )
      SET
        ${transform_fields} 
    `
  }
    
module.exports = {
  query_load_lead_data,
};

// @booking_created_on_utc,
// @min_lead_created_on_pst, -- fix Thu Dec 05 2024 12:02:52 GMT-0700 (Mountain Standard Time)
// @min_call_log_min_created_on_pst, -- fix 

