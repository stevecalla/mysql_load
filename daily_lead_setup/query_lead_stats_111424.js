function query_lead_stats() {
    return `
        -- #4) LEADS BY CREATED DATE, BY COUNTRY, BY SOURCE
        -- SELECT DISTINCT(renting_in_country), COUNT(*) FROM leads_master GROUP BY 1;

        SELECT 
            DATE_FORMAT(lm.created_on, '%Y-%m-%d') AS created_on, -- in GST, not in UST so no timezone conversion needed
            lm.renting_in_country,
            ls.source_name,
            COUNT(*) AS count_leads
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