// CREATE DATE IN GST
// CREATE VARIABLES FOR THE LAST 

function query_daily_booking_data() {
    return `
        -- BOOKINGS BY HOUR BUCKET BY DAY OF THE WEEK; FOR ALL HOUR BOOKING

        SELECT
            DATE_FORMAT(DATE_ADD(b.created_on, INTERVAL 4 HOUR), '%H') AS booking_time_bucket,
            -- FORMAT(COUNT(*), 0) AS total_count,
            -- Pivot for each unique booking_date EXCLUDING CANCELLED
            SUM(CASE WHEN rs.status != "Cancelled by User" AND DATE_FORMAT(DATE_ADD(b.created_on, INTERVAL 4 HOUR), '%Y-%m-%d') = '2024-09-09' THEN 1 ELSE 0 END) AS '2024-09-09',
            SUM(CASE WHEN rs.status != "Cancelled by User" AND DATE_FORMAT(DATE_ADD(b.created_on, INTERVAL 4 HOUR), '%Y-%m-%d') = '2024-09-10' THEN 1 ELSE 0 END) AS '2024-09-10',
            SUM(CASE WHEN rs.status != "Cancelled by User" AND DATE_FORMAT(DATE_ADD(b.created_on, INTERVAL 4 HOUR), '%Y-%m-%d') = '2024-09-11' THEN 1 ELSE 0 END) AS '2024-09-11',
            SUM(CASE WHEN rs.status != "Cancelled by User" AND DATE_FORMAT(DATE_ADD(b.created_on, INTERVAL 4 HOUR), '%Y-%m-%d') = '2024-09-12' THEN 1 ELSE 0 END) AS '2024-09-12',
            SUM(CASE WHEN rs.status != "Cancelled by User" AND DATE_FORMAT(DATE_ADD(b.created_on, INTERVAL 4 HOUR), '%Y-%m-%d') = '2024-09-13' THEN 1 ELSE 0 END) AS '2024-09-13',
            SUM(CASE WHEN rs.status != "Cancelled by User" AND DATE_FORMAT(DATE_ADD(b.created_on, INTERVAL 4 HOUR), '%Y-%m-%d') = '2024-09-14' THEN 1 ELSE 0 END) AS '2024-09-14',
            SUM(CASE WHEN rs.status != "Cancelled by User" AND DATE_FORMAT(DATE_ADD(b.created_on, INTERVAL 4 HOUR), '%Y-%m-%d') = '2024-09-15' THEN 1 ELSE 0 END) AS '2024-09-15',
            SUM(CASE WHEN rs.status != "Cancelled by User" AND DATE_FORMAT(DATE_ADD(b.created_on, INTERVAL 4 HOUR), '%Y-%m-%d') = '2024-09-16' THEN 1 ELSE 0 END) AS '2024-09-16',
            -- SUM(CASE WHEN rs.status != "Cancelled by User" THEN 1 ELSE 0 END) AS total_count_excluding_cancel,
            DATE_ADD(NOW(), INTERVAL 4 HOUR) AS date_time_now_gst,
            DATE_FORMAT(DATE_ADD(NOW(), INTERVAL 4 HOUR), '%Y-%m-%d') AS date_now_gst,
            HOUR(NOW()) + 4 AS current_hour

        FROM rental_car_booking2 AS b
            LEFT JOIN rental_status AS rs ON b.status = rs.id
            INNER JOIN myproject.rental_city rc ON rc.id = b.city_id
            INNER JOIN myproject.rental_country co ON co.id = rc.CountryID

        WHERE 
            DATE_FORMAT(DATE_ADD(b.created_on, INTERVAL 4 HOUR), '%Y-%m-%d') >= (
                SELECT 
                    DATE_FORMAT(DATE_SUB(DATE_ADD(MAX(b.created_on), INTERVAL 4 HOUR), INTERVAL 8 DAY), '%Y-%m-%d')
                FROM rental_car_booking2 AS b
            )
            and co.name IN ('United Arab Emirates')
            
            -- AND 
            -- DATE_FORMAT(DATE_ADD(b.created_on, INTERVAL 4 HOUR), '%H') < (HOUR(NOW()) + 4)

        GROUP BY DATE_FORMAT(DATE_ADD(b.created_on, INTERVAL 4 HOUR), '%H') WITH ROLLUP
        ORDER BY DATE_FORMAT(DATE_ADD(b.created_on, INTERVAL 4 HOUR), '%H');
        -- LIMIT 1000;
    `;
}

module.exports = {
    query_daily_booking_data,
}