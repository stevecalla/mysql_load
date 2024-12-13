const derived_fields = `
  created_on_pst DATE NOT NULL,

  booking_id_bm VARCHAR(50),

  lead_id_lm_list VARCHAR(255),
  lead_id VARCHAR(50),

  created_on_pst_lm_list VARCHAR(255),
  created_on_timestamp_pst_lm VARCHAR(255),

  created_on_utc_bm_list VARCHAR(255),
  booking_created_on_utc_bm VARCHAR(255),
  created_on_pst_bm_list VARCHAR(255),
  booking_created_on_pst_bm VARCHAR(255),

  rental_status VARCHAR(255),
  lead_status_id VARCHAR(255),

  renting_in_country VARCHAR(255),
  renting_in_country_abb VARCHAR(10),
  renting_in_country_list_lm VARCHAR(255),
  renting_in_country_lm VARCHAR(255),
  country_list_bm VARCHAR(255),
  country_bm VARCHAR(255),
  
  source_name_list_lm VARCHAR(255),
  source_name_lm VARCHAR(255),

  min_lead_created_on_pst VARCHAR(255),
  min_created_on_pst_list_cl VARCHAR(255),
  min_created_on_pst_cl VARCHAR(255),

  response_time VARCHAR(50),
  response_time_bin VARCHAR(255),

  shift_list VARCHAR(255),
  shift VARCHAR(50),

  max_created_on_gst VARCHAR(255),

  count_bookings INT,
  count_leads INT,
  
  created_on_timestamp_utc VARCHAR(255)
`;

const index_fields = `
  PRIMARY KEY (lead_id),
  INDEX idx_created_on_pst (created_on_pst),
  INDEX idx_booking_id_bm (booking_id_bm),
  INDEX idx_rental_status (rental_status),
  INDEX idx_lead_status_id (lead_status_id),
  INDEX idx_renting_in_country (renting_in_country),
  INDEX idx_renting_in_country_abb (renting_in_country_abb),
  INDEX idx_source_name_lm (source_name_lm),
  INDEX idx_booking_created_on_utc_bm (booking_created_on_utc_bm),
  INDEX idx_shift (shift),
  INDEX idx_response_time_bin (response_time_bin)
`;

const table = `lead_response_data`;

const query_create_lead_response_table = `
  CREATE TABLE IF NOT EXISTS ${table} (
    ${derived_fields},
    ${index_fields}
  );
`;

const tables_library = [
  { 
    table_name: table,
    create_query: query_create_lead_response_table,
    step: "STEP #2.1:",
    step_info: table,
  },
];

module.exports = {
  tables_library,
  query_create_lead_response_table,
};