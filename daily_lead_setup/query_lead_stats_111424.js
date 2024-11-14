function query_lead_stats() {
    return `
        -- #4) LEADS BY CREATED DATE, BY COUNTRY, BY SOURCE
        SELECT 
            DATE_FORMAT(lm.created_on, '%Y-%m-%d') AS created_on, -- in GST, not in UST so no timezone conversion needed
            lm.renting_in_country,
            ls.source_name,
            COUNT(*) AS count_leads,
            -- CURRENT DATE / TIME GST
            now() AS now_utc,
            DATE_FORMAT(DATE_ADD(NOW(), INTERVAL 4 HOUR), '%Y-%m-%d %H:%i:%s') AS created_at_gst,
            -- Max created_on for all records (without per-grouping)
            (SELECT 
                DATE_FORMAT(MAX(created_on), '%Y-%m-%d %H:%i:%s') 
            FROM leads_master 
            WHERE created_on >= CURRENT_DATE() - INTERVAL 5 DAY 
            AND created_on < CURRENT_DATE() + INTERVAL 1 DAY
            ) AS max_created_on

        FROM leads_master AS lm
            LEFT JOIN lead_sources AS ls ON lm.lead_source_id = ls.id
        WHERE 
            lm.created_on >= CURRENT_DATE() - INTERVAL 5 DAY
            AND lm.created_on < CURRENT_DATE() + INTERVAL 1 DAY  -- Ensures the current day's records are also included
        GROUP BY 1 DESC, 2, 3;
    `;
}

module.exports = {
    query_lead_stats,
}