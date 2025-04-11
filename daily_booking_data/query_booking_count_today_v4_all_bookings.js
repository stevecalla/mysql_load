// CREATE DATE IN GST
// CREATE VARIABLES FOR THE LAST 

function query_booking_count_today_v4_all_bookings() {
    return `
        WITH bookings_last_two_days_cte AS (
            SELECT
                DATE_FORMAT(DATE_ADD(b.created_on, INTERVAL 4 HOUR), '%Y-%m-%d') AS booking_date_gst,
                co.name AS delivery_country,
                CASE
                    WHEN rs.status = "Cancelled by User" THEN 'cancelled'
                    ELSE 'not_cancelled'
                END AS booking_status_category,
                CASE
                    WHEN b.early_return = 0 THEN
                        CASE
                            WHEN b.days < 7 THEN 'daily'
                            WHEN b.days > 29 AND b.is_subscription = 1 THEN 'Subscription'
                            WHEN b.days > 29 THEN 'Monthly'
                            ELSE 'Weekly'
                        END
                    ELSE 
                        CASE
                            WHEN erb.new_days < 7 THEN 'daily'
                            WHEN erb.new_days > 29 AND b.is_subscription = 1 THEN 'Subscription'
                            WHEN erb.new_days > 29 THEN 'Monthly'
                            ELSE 'Weekly'
                        END
                END AS booking_type,
                b.created_on,
                b.updated_on,
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
                LEFT JOIN rental_early_return_bookings AS erb ON erb.booking_id = b.id AND erb.is_active = 1 
                LEFT JOIN rental_status AS rs ON b.status = rs.id
                INNER JOIN myproject.rental_city rc ON rc.id = b.city_id
                INNER JOIN myproject.rental_country co ON co.id = rc.CountryID
            WHERE 
                -- rs.status != "Cancelled by User"
                -- AND
            
                -- CREATED ON DATE IS WITHIN THE LAST 7 DAYS
                DATE_FORMAT(DATE_ADD(b.created_on, INTERVAL 4 HOUR), '%Y-%m-%d') >= (
                    SELECT DATE_FORMAT(
                            DATE_SUB(DATE_ADD(MAX(b.created_on), INTERVAL 4 HOUR), INTERVAL 1 DAY),
                            '%Y-%m-%d'
                        )
                    FROM rental_car_booking2 AS b
                )        
            -- LIMIT 100
        ) 
        SELECT 
            booking_date_gst,
            delivery_country,
            booking_status_category,
            CASE 
                WHEN booking_type IN ('daily') THEN 'daily'
                WHEN booking_type IN ('Weekly') THEN 'weekly'
                WHEN booking_type IN ('Monthly') THEN 'monthly'
                WHEN booking_type IN ('Subscription') THEN 'sub'
                ELSE 0
            END AS booking_type,
            COUNT(*) AS current_bookings,
            DATE_FORMAT(DATE_ADD(NOW(), INTERVAL 4 HOUR), '%Y-%m-%d %H:%i:%s') AS created_at_gst,
            GREATEST (
                DATE_FORMAT(DATE_ADD(MAX(created_on), INTERVAL 4 HOUR), '%Y-%m-%d %H:%i:%s'),
                DATE_FORMAT(DATE_ADD(MAX(updated_on), INTERVAL 4 HOUR), '%Y-%m-%d %H:%i:%s')
            ) AS most_recent_event_update,
            date_most_recent_created_on_gst,
            date_most_recent_updated_on_gst

        FROM bookings_last_two_days_cte

        GROUP BY booking_date_gst, delivery_country, booking_status_category, booking_type
        ORDER BY booking_date_gst, delivery_country, booking_status_category, booking_type
        -- LIMIT 100
        ;
    `;
}

module.exports = {
    query_booking_count_today_v4_all_bookings,
}