//record only exists for 22919
function generate_distinct_list() {
    let query_distinct_pickup_month_year = `SELECT DISTINCT 
        pickup_month_year

        FROM pacing_base_groupby 

        WHERE 
        pickup_month_year NOT LIKE 'N/A' 
        AND pickup_month_year IS NOT NULL 
        AND pickup_month_year != ''

        ORDER BY pickup_month_year;
    `;

    // console.log(query_distinct_pickup_month_year);
    return query_distinct_pickup_month_year;
}

// generate_distinct_list();

module.exports = { generate_distinct_list };