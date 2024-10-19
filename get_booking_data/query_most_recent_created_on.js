// const query_most_recent_create_on_date = `
//     -- FINDS THE MOST RECENT CREATED RECORD AND UPDATED RECORD; INDICATES WHEN THE DB WAS LAST UPDATED
//     SELECT 
//         source_field, 
        
//         -- last updated dates
//         most_recent_event_update, -- converts base time via MST + 7 to UTC (converts 10:07:53 to 16:07:53)
//         -- DATE_FORMAT(CONVERT_TZ(most_recent_event_update, '+00:00', '+00:00'), '%Y-%m-%d %H:%i:%s UTC') AS most_recent_event_update_utc, -- military time format
//         DATE_FORMAT(CONVERT_TZ(most_recent_event_update, '+00:00', '+00:00'), '%Y-%m-%d %h:%i:%s %p UTC') AS most_recent_event_update_utc, -- am/pm format

//         -- current timestamp dates
//         CURRENT_TIMESTAMP AS execution_timestamp,
//         -- DATE_FORMAT(CONVERT_TZ(CURRENT_TIMESTAMP, '+00:00', '+00:00'), '%Y-%m-%d %H:%i:%s UTC') AS execution_timestamp_utc, -- military time format
//         DATE_FORMAT(CONVERT_TZ(CURRENT_TIMESTAMP, '+00:00', '+00:00'), '%Y-%m-%d %h:%i:%s %p UTC') AS execution_timestamp_utc, -- am/pm format

//         -- variance between last updated and current timestamp
//         TIMESTAMPDIFF(HOUR, most_recent_event_update, CURRENT_TIMESTAMP()) as time_stamp_difference_hour,
//         TIMESTAMPDIFF(MINUTE, most_recent_event_update, CURRENT_TIMESTAMP()) as time_stamp_difference_minute,
//         -- TIMESTAMPDIFF(HOUR, '2024-10-19 03:41:54', '2024-10-19 03:51:54') as time_stamp_difference_hour_test, -- in whole hour
//         -- TIMESTAMPDIFF(MINUTE, '2024-10-19 03:41:54', '2024-10-19 05:50:54') as time_stamp_difference_minutes_test, -- in minutes
//         CASE
//             WHEN TIMESTAMPDIFF(HOUR, most_recent_event_update, CURRENT_TIMESTAMP()) <= 2 THEN "true"
//             ELSE "false"
//         END AS is_within_2_hours,
//         CASE
//             WHEN TIMESTAMPDIFF(MINUTE, most_recent_event_update, CURRENT_TIMESTAMP()) <= 15 THEN "true"
//             ELSE "false"
//         END AS is_within_15_minutes,
        
//         most_recent_created_on,
//         most_recent_updated_on

//     FROM (
//             SELECT 
//                 'created_on' AS source_field, 
//                 MAX(created_on) AS most_recent_event_update, 
//                 MAX(created_on) AS most_recent_created_on, 
//                 MAX(updated_on) AS most_recent_updated_on,
//                 count(*) AS count
//             FROM myproject.rental_car_booking2

//         UNION ALL

//             SELECT 
//                 'updated_on' AS source_field, 
//                 MAX(updated_on) AS most_recent_event_update, 
//                 MAX(created_on) AS most_recent_created_on, 
//                 MAX(updated_on) AS most_recent_updated_on, 
//                 count(*) AS count
//             FROM myproject.rental_car_booking2

//         ) AS last_updated_table
//     ORDER BY most_recent_event_update DESC
//     LIMIT 1; -- keeps most recent created on record
// `;

// module.exports = { query_most_recent_create_on_date };