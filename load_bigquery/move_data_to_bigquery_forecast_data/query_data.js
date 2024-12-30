const forecastDataQuery = `
    SELECT 
        DATE_FORMAT(booking_date, '%Y-%m-%d') AS booking_date_gst, -- date '2024-02-12'
        booking_time_bucket,
        segment_major,
        segment_minor,
        hourly_bookings,
        booking_date_day_of_week,

        DATE_FORMAT(today_date_gst, '%Y-%m-%d') AS today_date_gst, -- date '2024-02-12'

        today_timestamp_utc,
        today_timestamp_gst,                    	
        today_current_hour_gst,

        booking_time_bucket_flag,

        today_current_day_of_week_gst,	
        
        DATE_FORMAT(same_day_last_week, '%Y-%m-%d') AS same_day_last_week_gst, -- date '2024-02-12'

        created_at_gst

    FROM ezhire_forecast_data.booking_by_hour_data
`;

const forecastSummaryMetricsQuery = `
    SELECT
        DATE_FORMAT(today_date_gst, '%Y-%m-%d') AS today_date_gst, -- date '2024-02-12'

        today_current_day_of_week_gst,
        today_current_hour_gst,
        segment_major,
        segment_minor,

        DATE_FORMAT(booking_date, '%Y-%m-%d') AS booking_date_gst, -- date '2024-02-12'

        booking_date_day_of_week,
        booking_total_prior_to_current_hour,
        booking_total,
        created_at_gst

    FROM ezhire_forecast_data.forecast_summary_metrics_data
`;

module.exports = {
    forecastDataQuery,
    forecastSummaryMetricsQuery,
}