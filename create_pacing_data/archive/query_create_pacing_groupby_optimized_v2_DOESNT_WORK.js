function query_pacing_groupby_optimized() {
    let query_pacing_groupby_optimized = `
    -- Initialize variables
    SET @running_total_booking_count := 0;
    SET @running_total_booking_charge_aed := 0;
    SET @running_total_booking_charge_less_discount_aed := 0;
    SET @running_total_booking_charge_less_discount_extension_aed := 0;
    SET @running_total_extension_charge_aed := 0;
    SET @prev_month_year := NULL;
    
    -- Create table and insert data
    CREATE TABLE pacing_base_groupby AS
    SELECT
        pickup_month_year,
        booking_date,
        days_from_first_day_of_month,
        count,
        FORMAT(booking_charge_aed, 0) AS total_booking_charge_aed,
        FORMAT(booking_charge_less_discount_aed, 0) AS total_booking_charge_less_discount_aed,
        FORMAT(booking_charge_less_discount_extension_aed, 0) AS total_booking_charge_less_discount_extension_aed,
        FORMAT(extension_charge_aed, 0) AS total_extension_charge_aed,
        
        -- Update variables using SET statements
        FORMAT(
            @running_total_booking_count := IF(@prev_month_year = pickup_month_year,
                @running_total_booking_count + count,
                count
            ), 0
        ) AS running_total_booking_count,
        FORMAT(
            @running_total_booking_charge_aed := IF(@prev_month_year = pickup_month_year,
                @running_total_booking_charge_aed + booking_charge_aed,
                booking_charge_aed
            ), 0
        ) AS running_total_booking_charge_aed,
        FORMAT(
            @running_total_booking_charge_less_discount_aed := IF(@prev_month_year = pickup_month_year,
                @running_total_booking_charge_less_discount_aed + booking_charge_less_discount_aed,
                booking_charge_less_discount_aed
            ), 0
        ) AS running_total_booking_charge_less_discount_aed,
        FORMAT(
            @running_total_booking_charge_less_discount_extension_aed := IF(@prev_month_year = pickup_month_year,
                @running_total_booking_charge_less_discount_extension_aed + booking_charge_less_discount_extension_aed,
                booking_charge_less_discount_extension_aed
            ), 0
        ) AS running_total_booking_charge_less_discount_extension_aed,
        FORMAT(
            @running_total_extension_charge_aed := IF(@prev_month_year = pickup_month_year,
                @running_total_extension_charge_aed + extension_charge_aed,
                extension_charge_aed
            ), 0
        ) AS running_total_extension_charge_aed,
        @prev_month_year := pickup_month_year AS dummy_variable
    FROM (
        SELECT
            pb.pickup_month_year,
            pb.booking_date,
            pb.days_from_first_day_of_month,
            SUM(count) AS count,
            SUM(booking_charge_aed) AS booking_charge_aed,
            SUM(booking_charge_less_discount_aed) AS booking_charge_less_discount_aed,
            SUM(booking_charge_less_discount_extension_aed) AS booking_charge_less_discount_extension_aed,
            SUM(extension_charge_aed) AS extension_charge_aed
        FROM ezhire_pacing_metrics.pacing_base pb
        GROUP BY 
            pb.pickup_month_year,
            pb.booking_date,  
            pb.days_from_first_day_of_month
        ORDER BY pb.pickup_month_year ASC
        -- LIMIT 1000
    ) AS subquery;

    -- WARNINGS
    SHOW WARNINGS;
    `;
    
    return query_pacing_groupby_optimized;
}

// let x = query_pacing_groupby_optimized();
// console.log(x);

module.exports = { query_pacing_groupby_optimized };