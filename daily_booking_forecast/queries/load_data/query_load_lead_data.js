const derived_fields = `
  booking_date,

  booking_time_bucket,
  segment_major,
  segment_minor,
  hourly_bookings,
  booking_date_day_of_week,

  today_date_gst,
  today_timestamp_utc,
  today_timestamp_gst,

  today_current_hour_gst,
  booking_time_bucket_flag,
  today_current_day_of_week_gst,

  same_day_last_week,
  created_at_gst
`;

const transform_fields = `
`;

function query_load_booking_by_hour_data(filePath, table_name) {
  return `
    LOAD DATA LOCAL INFILE '${filePath}'
    INTO TABLE ${table_name}
    FIELDS TERMINATED BY ','
    ENCLOSED BY '"'
    LINES TERMINATED BY '\\n'
    IGNORE 1 LINES
    (
      ${derived_fields} 
    )
  `
}

module.exports = {
  query_load_booking_by_hour_data,
};

