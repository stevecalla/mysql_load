async function query_slack_daily_forecast_data() {
    return `
        -- C:\Users\calla\development\ezhire\mysql_queries\daily_booking_forecast\discovery_query_forecast_metrics_123024.sql
    
        SELECT
            segment_major,
            segment_minor, 
            today_current_hour_gst,
            SUM(CASE WHEN booking_time_bucket_flag IN ("yes") THEN hourly_bookings ELSE 0 END) booking_total_prior_to_current_hour,
            SUM(hourly_bookings) AS booking_total
            
        FROM booking_by_hour_data
        WHERE segment_minor NOT IN ('actual_last_7_days', 'actuals_same_day_last_4_weeks')
        GROUP BY segment_major, segment_minor, today_current_hour_gst;
    `;
}

module.exports = {
    query_slack_daily_forecast_data,
}