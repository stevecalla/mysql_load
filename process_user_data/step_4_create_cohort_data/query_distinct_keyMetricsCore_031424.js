function generate_distinct_list(distinct_field) {

    let query_distinct_keyMetrics_core = `SELECT DISTINCT 
            -- vendor
            --  booking_type
            --  status
            --  country
            ${distinct_field}
        -- FROM key_metrics_base
        FROM user_data_cohort_base
        WHERE vendor NOT LIKE 'N/A' AND vendor IS NOT NULL AND vendor != ''
        ORDER BY ${distinct_field};
        -- LIMIT 100;
    `;

    // console.log(query_distinct_keyMetrics_core);
    return query_distinct_keyMetrics_core;
}

// let distinctField = "vendor";
// generate_distinct_list(distinctField);

module.exports = { 
    generate_distinct_list 
};
