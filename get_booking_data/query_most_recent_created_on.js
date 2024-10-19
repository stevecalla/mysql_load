const query_most_recent_create_on_date = `
    -- FINDS THE MOST RECENT CREATED RECORD AND UPDATED RECORD; INDICATES WHEN THE DB WAS LAST UPDATED
    SELECT 
        last_updated, -- converts base time via MST + 7 to UTC (converts 10:07:53 to 16:07:53)

        DATE_FORMAT(CONVERT_TZ(last_updated, '+00:00', '+00:00'), '%Y-%m-%d %H:%i:%s UTC') AS last_updated_utc,

        source_field, 
        
        CURRENT_TIMESTAMP AS execution_timestamp,
        DATE_FORMAT(CONVERT_TZ(CURRENT_TIMESTAMP, '+00:00', '+00:00'), '%Y-%m-%d %H:%i:%s UTC') AS execution_timestamp_utc,
        TIMESTAMPDIFF(HOUR, last_updated, CURRENT_TIMESTAMP()) as time_stamp_difference,
        CASE
            WHEN TIMESTAMPDIFF(HOUR, last_updated, CURRENT_TIMESTAMP()) <= 2 THEN "true"
            ELSE "false"
        END AS is_within_2_hours

    FROM (
        SELECT MAX(created_on) AS last_updated, 'created_on' AS source_field, count(*) AS count
            FROM myproject.rental_car_booking2
        UNION ALL
        SELECT MAX(updated_on) AS last_updated, 'updated_on' AS source_field, count(*) AS count
            FROM myproject.rental_car_booking2
    ) AS last_updated_table
    ORDER BY last_updated DESC
    LIMIT 1;
`;

module.exports = { query_most_recent_create_on_date };