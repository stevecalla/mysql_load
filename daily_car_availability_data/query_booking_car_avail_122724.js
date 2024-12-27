function query_car_availability() {
    return `
        -- C:\Users\calla\development\ezhire\mysql_queries\car_fleet_level\discovery_car_availability_v2_cte_122624.sql
        
        WITH tmp_car AS (
            SELECT 
                MAX(cs.id) AS maxid,
                cs.car_id
            FROM rental_booking_car_driver_status cs
                INNER JOIN rental_car cr ON cs.car_id = cr.id
            WHERE cr.user_id = 234555  -- ezhire dispatch center vendor ID; lookup table is rental_vendors
            GROUP BY cs.car_id
        ),
        status_summary AS (
            SELECT 
                COUNT(cr.id) AS status_count,
                COALESCE(cr_st.car_status, 0) AS car_status,
                COALESCE(cr_st.reason_id, 0) AS car_reason,
                (CASE WHEN COALESCE(cr.replacement_of, 0) = 0 THEN 0 ELSE 1 END) AS replacement_of
            FROM rental_car cr
                INNER JOIN tmp_car tc ON cr.id = tc.car_id
                INNER JOIN rental_booking_car_driver_status cr_st ON cr_st.id = tc.maxid
            WHERE 
                cr.user_id = 234555 -- ezhire dispatch center vendor ID; lookup table is rental_vendors
                AND cr.status = 1
            GROUP BY 
                COALESCE(cr_st.car_status, 0),
                COALESCE(cr_st.reason_id, 0),
                (CASE WHEN COALESCE(cr.replacement_of, 0) = 0 THEN 0 ELSE 1 END)
        ),
        aggregated_status AS (
            SELECT
                SUM(CASE WHEN car_status IN (9, 14, 15, 16, 21, 23) AND car_reason <> 9 THEN status_count ELSE 0 END) AS count_total_available,
                SUM(CASE WHEN car_status IN (2, 3, 4, 10, 11, 31, 17, 18, 19) THEN status_count ELSE 0 END) AS count_total_on_rent,
                SUM(CASE WHEN car_status IN (22) AND replacement_of = 1 THEN status_count ELSE 0 END) AS count_ncm_rp_out,
                SUM(CASE WHEN car_status IN (22) AND replacement_of = 0 THEN status_count ELSE 0 END) AS count_ncm_dc_out,
                (
                    SELECT COUNT(c.id) AS status_count 
                    FROM rental_car c 
                    WHERE 
                        c.user_id = 234555 
                        AND c.replacement_status IN (4, 9, 7, 8, 11)
                ) AS count_other
            FROM status_summary
        )
        SELECT 
            * ,
            (count_total_available + count_total_on_rent + count_ncm_rp_out + count_ncm_dc_out + count_other) AS count_total_cars,
            (count_total_on_rent / (count_total_available + count_total_on_rent + count_ncm_rp_out + count_ncm_dc_out + count_other)) * 100 AS utilization_total
        FROM aggregated_status;
    `;
}

module.exports = {
    query_car_availability,
}