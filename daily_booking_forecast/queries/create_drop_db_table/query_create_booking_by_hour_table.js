const derived_fields = `
  booking_date DATE,

  booking_time_bucket INT,
  segment_major VARCHAR(50),
  segment_minor VARCHAR(50),
  hourly_bookings INT,
  booking_date_day_of_week INT,

  today_date_gst DATE,
  today_timestamp_utc TIMESTAMP,
  today_timestamp_gst TIMESTAMP,

  today_current_hour_gst INT,
  booking_time_bucket_flag VARCHAR(10),
  today_current_day_of_week_gst INT,

  same_day_last_week DATE,
  created_at_gst TIMESTAMP
`;

const index_fields = `
  INDEX idx_booking_date (booking_date),
  INDEX idx_time_bucket (booking_time_bucket),
  INDEX idx_segment_major (segment_major),
  INDEX idx_segment_minor (segment_minor),
  INDEX idx_day_of_week (booking_date_day_of_week)
`;

const table = `booking_by_hour_data`;

const query_create_booking_by_hour_table = `
  CREATE TABLE IF NOT EXISTS ${table} (
    ${derived_fields},
    ${index_fields}
  );
`;

const tables_library = [
  { 
    table_name: table,
    create_query: query_create_booking_by_hour_table,
    step: "STEP #2.1:",
    step_info: table,
  },
];

module.exports = {
  tables_library,
  query_create_booking_by_hour_table
};