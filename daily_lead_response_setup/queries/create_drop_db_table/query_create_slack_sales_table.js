const derived_fields = `
  created_on_pst DATE NOT NULL,
  Booking_id VARCHAR(50),
  rental_status VARCHAR(255),
  lead_status_id VARCHAR(255),
  lead_id INT,
  renting_in_country VARCHAR(255),
  source_name VARCHAR(255),
  booking_created_on_utc DATETIME,
  count_lead_id INT,
  min_lead_created_on_pst DATETIME,
  min_call_log_min_created_on_pst DATETIME,
  response_time VARCHAR(50),
  response_time_bin VARCHAR(255),
  shift VARCHAR(50),
  query_source VARCHAR(255),
  max_created_on_gst DATETIME
`;

const index_fields = `
  PRIMARY KEY (lead_id),

  -- Individual Indexes
  INDEX idx_booking_id (Booking_id),
  INDEX idx_created_on_pst (created_on_pst),
  INDEX idx_rental_status (rental_status),
  INDEX idx_lead_status_id (lead_status_id),
  INDEX idx_renting_in_country (renting_in_country),
  INDEX idx_source_name (source_name),
  INDEX idx_booking_created_on_utc (booking_created_on_utc),
  INDEX idx_shift (shift),
  INDEX idx_response_time_bin (response_time_bin)
`;

const table = `lead_response_data`;

const query_create_lead_response_table = `
  CREATE TABLE IF NOT EXISTS ${table} (
    ${derived_fields}
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
