const derived_fields = `
    created_on_pst,
    Booking_id,
    rental_status,
    lead_status_id,
    lead_id,
    renting_in_country,
    source_name,
    booking_created_on_utc,
    count_lead_id,
    @min_lead_created_on_pst, -- fix Thu Dec 05 2024 12:02:52 GMT-0700 (Mountain Standard Time)
    @min_call_log_min_created_on_pst, -- fix 
    response_time,
    response_time_bin,
    shift,
    query_source,
    max_created_on_gst
`;

const transform_fields = `
-- CONVERTS "Fri Jun 11 2021 12:03:17 GMT-0600 (Mountain Daylight Time)" TO '2021-06-11 12:03:17' TO MAINTAIN MTN TIME
    min_lead_created_on_pst = CASE 
        WHEN @min_lead_created_on_pst IS NOT NULL AND @min_lead_created_on_pst != 'Invalid Date' THEN 
            STR_TO_DATE(SUBSTRING_INDEX(SUBSTRING_INDEX(@min_lead_created_on_pst, ' GMT', 1), ' ', -5), '%a %b %d %Y %H:%i:%s')
        ELSE
            NULL
    END,
    min_call_log_min_created_on_pst = CASE
        WHEN @min_call_log_min_created_on_pst IS NOT NULL AND @min_call_log_min_created_on_pst != 'Invalid Date' THEN 
            STR_TO_DATE(SUBSTRING_INDEX(SUBSTRING_INDEX(@min_call_log_min_created_on_pst, ' GMT', 1), ' ', -5), '%a %b %d %Y %H:%i:%s')
        ELSE
            NULL
    END;
`;

function query_load_lead_data(filePath, table) {
  return `
    LOAD DATA LOCAL INFILE '${filePath}'
    INTO TABLE ${table}
    FIELDS TERMINATED BY ','
    ENCLOSED BY ''
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
    

