const query_most_recent_create_on_date = `
    -- FINDS THE MOST RECENT CREATED RECORD AND UPDATED RECORD; INDICATES WHEN THE DB WAS LAST UPDATED
    SELECT 
        source_field, 
	    DATE_FORMAT(CURRENT_TIMESTAMP(), '%Y-%m-%d %h:%i:%s %p') AS current_timestamp_utc,
    
        -- last updated dates
        most_recent_event_update, -- gst
        DATE_FORMAT(most_recent_event_update, '%Y-%m-%d %h:%i:%s %p GST') AS most_recent_event_update_gst, -- am/pm format

        -- current timestamp dates
        DATE_FORMAT(DATE_ADD(MAX(CURRENT_TIMESTAMP), INTERVAL 4 HOUR), '%Y-%m-%d %h:%i:%s %p GST') AS execution_timestamp_gst, -- am/pm format

        -- variance between last updated and current timestamp
        TIMESTAMPDIFF(HOUR, most_recent_event_update, DATE_ADD(CURRENT_TIMESTAMP, INTERVAL 4 HOUR)) AS time_stamp_difference_hour,
        TIMESTAMPDIFF(MINUTE, most_recent_event_update, DATE_ADD(CURRENT_TIMESTAMP, INTERVAL 4 HOUR)) AS time_stamp_difference_minute,

        CASE
            WHEN TIMESTAMPDIFF(HOUR, most_recent_event_update, DATE_ADD(CURRENT_TIMESTAMP, INTERVAL 4 HOUR)) <= 2 THEN 1
            ELSE 0
        END AS is_within_2_hours,
        CASE
            WHEN TIMESTAMPDIFF(MINUTE, most_recent_event_update, DATE_ADD(CURRENT_TIMESTAMP, INTERVAL 4 HOUR)) <= 15 THEN 1
            ELSE 0
        END AS is_within_15_minutes,
        
        most_recent_created_on,
        most_recent_updated_on

    FROM 
    (
            SELECT 
                'created_on' AS source_field, 
                DATE_FORMAT(DATE_ADD(MAX(created_on), INTERVAL 4 HOUR), '%Y-%m-%d %H:%i:%s') AS most_recent_event_update, -- gst
                DATE_FORMAT(DATE_ADD(MAX(created_on), INTERVAL 4 HOUR), '%Y-%m-%d %H:%i:%s') AS most_recent_created_on, -- gst
                DATE_FORMAT(DATE_ADD(MAX(updated_on), INTERVAL 4 HOUR), '%Y-%m-%d %H:%i:%s') AS most_recent_updated_on, -- gst
                count(*) AS count
            FROM myproject.rental_car_booking2

        UNION ALL

            SELECT 
                'updated_on' AS source_field, 
                DATE_FORMAT(DATE_ADD(MAX(updated_on), INTERVAL 4 HOUR), '%Y-%m-%d %H:%i:%s') AS most_recent_event_update, -- gst
                DATE_FORMAT(DATE_ADD(MAX(created_on), INTERVAL 4 HOUR), '%Y-%m-%d %H:%i:%s') AS most_recent_created_on, -- gst
                DATE_FORMAT(DATE_ADD(MAX(updated_on), INTERVAL 4 HOUR), '%Y-%m-%d %H:%i:%s') AS most_recent_updated_on, -- gst
                count(*) AS count
            FROM myproject.rental_car_booking2

        ) AS last_updated_table
    GROUP BY source_field
    ORDER BY most_recent_event_update DESC
    LIMIT 1
    ; -- keeps most recent created on record
`;

module.exports = { query_most_recent_create_on_date };