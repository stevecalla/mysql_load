function query_lead_stats() {
    return `
        -- LEADS BY COUNTRY, SOURCE WITH LEADS COUNT, BOOKING COUNTS FOR CANCEL, CONFIRMED, TOTAL
        SELECT 
            DATE_FORMAT(lm.created_on, '%Y-%m-%d') AS created_on_gst, -- in GST, not in UST so no timezone conversion needed
            lm.renting_in_country,
            -- st.lead_status,
            ls.source_name,

            COUNT(*) AS count_leads,
            SUM(CASE WHEN st.lead_status IN ('Booking Cancelled') THEN 1 ELSE 0 END) AS count_booking_cancelled,
            SUM(CASE WHEN st.lead_status IN ('Booking Confirmed') THEN 1 ELSE 0 END) AS count_booking_confirmed,
            SUM(CASE WHEN st.lead_status IN ('Booking Cancelled', 'Booking Confirmed') THEN 1 ELSE 0 END) AS count_booking_total,

            -- CURRENT DATE / TIME GST
            now(),
            CURRENT_DATE(),
            DATE_FORMAT(NOW(), '%Y-%m-%d %H:%i:%s') AS queried_at_utc,
            DATE_FORMAT(DATE_ADD(NOW(), INTERVAL 4 HOUR), '%Y-%m-%d %H:%i:%s') AS queried_at_gst,

            -- Max created_on for all records (without per-grouping)
            (SELECT 
                DATE_FORMAT(MAX(created_on), '%Y-%m-%d %H:%i:%s') 
            FROM leads_master 
            WHERE 
                created_on >= CURRENT_DATE() - INTERVAL 1 DAY 
                AND created_on < CURRENT_DATE() + INTERVAL 1 DAY
            ) AS max_created_on_gst

        FROM leads_master AS lm
            LEFT JOIN lead_sources AS ls ON lm.lead_source_id = ls.id
            LEFT JOIN lead_status AS st ON lm.lead_status_id = st.id
        WHERE 
            -- st.lead_status IN ('Booking Generated','Booking Confirmed','Booking Cancelled')
            -- AND 
            lm.created_on >= DATE_FORMAT(DATE_ADD(NOW(), INTERVAL 4 HOUR), '%Y-%m-%d') - INTERVAL 1 DAY
        GROUP BY 1, 2, 3;
    `;
}

module.exports = {
    query_lead_stats,
}