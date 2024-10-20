// CREATE DATE IN GST
// CREATE VARIABLES FOR THE LAST 

function query_booking_count_today_v2() {
    return `
        -- BOOKING COUNT FOR YESTERDAY AND TODAY WITH ALL COUNTRIES
        SELECT
            DATE_FORMAT(DATE_ADD(b.created_on, INTERVAL 4 HOUR), '%Y-%m-%d') AS booking_date_gst,
        
            -- DELIVERY COUNTRY
            co.name AS delivery_country,
            
            -- COUNT OF BOOKINGS
            COUNT(*) AS current_bookings,

            -- CURRENT DATE / TIME GST    
            DATE_FORMAT(DATE_ADD(NOW(), INTERVAL 4 HOUR), '%Y-%m-%d %H:%i:%s') AS created_at_gst,
        
		    GREATEST 
			(
				DATE_FORMAT(DATE_ADD(MAX(created_on), INTERVAL 4 HOUR), '%Y-%m-%d %H:%i:%s'),
				DATE_FORMAT(DATE_ADD(MAX(updated_on), INTERVAL 4 HOUR), '%Y-%m-%d %H:%i:%s')
			) AS most_recent_event_update,
            (
                SELECT 
                    DATE_FORMAT(DATE_ADD(MAX(created_on), INTERVAL 4 HOUR), '%Y-%m-%d %H:%i:%s')
                FROM rental_car_booking2 AS b
            ) AS date_most_recent_created_on_gst,
            (
                SELECT 
                    DATE_FORMAT(DATE_ADD(MAX(updated_on), INTERVAL 4 HOUR), '%Y-%m-%d %H:%i:%s')
                FROM rental_car_booking2 AS b
            ) AS date_most_recent_updated_on_gst

        FROM rental_car_booking2 AS b
            LEFT JOIN rental_status AS rs ON b.status = rs.id
            INNER JOIN myproject.rental_city rc ON rc.id = b.city_id
            INNER JOIN myproject.rental_country co ON co.id = rc.CountryID

        WHERE 
            rs.status != "Cancelled by User"
            AND
            
            -- CREATED ON DATE IS WITHIN THE LAST 7 DAYS
            DATE_FORMAT(DATE_ADD(b.created_on, INTERVAL 4 HOUR), '%Y-%m-%d') >= (
                SELECT 
                    DATE_FORMAT(DATE_SUB(DATE_ADD(MAX(b.created_on), INTERVAL 4 HOUR), INTERVAL 1 DAY), '%Y-%m-%d')
                FROM rental_car_booking2 AS b
            )
            -- AND 
            -- co.name IN ('United Arab Emirates')
            
        GROUP BY booking_date_gst, co.name WITH ROLLUP
        ORDER BY booking_date_gst, co.name
        -- LIMIT 100;
    `;
}

module.exports = {
    query_booking_count_today_v2,
}