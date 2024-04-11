// let variableList = [{ segment: "vendor", option: "Dispatch" }, { segment: "vendor", option: "Marketplace" }];
let repeatCode = "";
let baseCode = "";
const booking_date = '2023-01-01';
const pickup_date = '2023-01-01'; //not in use as a variable
const return_date = '2023-01-01';
const status = '%Cancel%';

function generateRepeatCode(variableList) {
    console.log('Generating code to get the on-rent data');

    for (let i = 0; i < variableList.length; i++) {
        // console.log(variableList[i].segment);
        // console.log(variableList[i].option);
    
        let injectCode = `LOWER(${variableList[i].segment}) LIKE LOWER('${variableList[i].option}')`;
        let fileName = `${variableList[i].segment}_on_rent_${variableList[i].option}`.replace(/\s+/g, '_').toLowerCase();
        let comma = `,`;

        //remove comma for final version because of SQL syntax
        if (i === variableList.length - 1) {
            comma = '';
        };
    
        let templateCode = `
            SUM(
                CASE
                    WHEN ct.calendar_date = km.pickup_date AND ${injectCode} THEN km.pickup_fraction_of_day
                    WHEN ct.calendar_date = km.return_date AND ${injectCode} THEN km.return_fraction_of_day
                    WHEN ct.calendar_date BETWEEN km.pickup_date AND km.return_date AND ${injectCode} THEN 1
                    ELSE 0
                END
            ) AS ${fileName}${comma}
            `;
    
        repeatCode += templateCode;
    
    };

    // console.log(repeatCode);
    return(generateBaseCode(repeatCode));
};

function generateBaseCode(repeatCode) {
    // CREATE TEMPORARY TABLE IF NOT EXISTS temp AS
    baseCode = `
        CREATE TABLE IF NOT EXISTS key_metrics_data AS
        SELECT
            km.created_at,
            DATE_FORMAT(DATE(ct.calendar_date), '%Y-%m-%d') AS calendar_date,     
            ct.year AS year,
            ct.quarter AS quarter,
            ct.month AS month,
            ct.week_of_year AS week,
            ct.day_of_year AS day,
            km.max_booking_datetime,
        
            -- CALC IS_TODAY
            CASE
                WHEN ct.calendar_date = DATE_FORMAT(km.max_booking_datetime, '%Y-%m-%d') THEN "yes"
                ELSE "no"
            END AS is_today,
            
            -- TOTAL ON-RENT CALCULATION
            COUNT(km.id) AS days_on_rent_whole_day,
        
            SUM(
                CASE
                    WHEN ct.calendar_date = km.pickup_date THEN km.pickup_fraction_of_day
                    WHEN ct.calendar_date = km.return_date THEN km.return_fraction_of_day
                    WHEN ct.calendar_date BETWEEN km.pickup_date AND km.return_date THEN 1
                    ELSE 0
                END
            ) AS days_on_rent_fraction,

            SUM(
                CASE
                    WHEN ct.calendar_date = km.pickup_date THEN 1
                    WHEN ct.calendar_date > km.pickup_date AND ct.day_of_year = 1 THEN 1
                    ELSE 0
                END
            ) AS trans_on_rent_count,

            -- BOOKING COUNT
            SUM(
                CASE
                    WHEN ct.calendar_date = km.booking_date THEN 1
                    ELSE 0
                END
            ) AS booking_count,  
        
            -- PICKUP COUNT
            SUM(
                CASE
                    WHEN ct.calendar_date = km.pickup_date THEN 1
                    ELSE 0
                END
            ) AS pickup_count,
        
            -- RETURN COUNT
            SUM(
                CASE
                    WHEN ct.calendar_date = km.return_date THEN 1
                    ELSE 0
                END
            ) AS return_count,

            -- INITIAL RENTAL PERIOD DAYS
            SUM(
                CASE
                    WHEN extension_days > 0
                        AND ct.calendar_date BETWEEN 
                        km.pickup_date AND DATE_ADD(km.pickup_date, INTERVAL (km.days_less_extension_days - 1) DAY)
                        THEN 1
        
                    WHEN extension_days = 0
                        AND ct.calendar_date BETWEEN             
                            km.pickup_date AND
                            km.return_date
                        THEN 1
        
                    ELSE 0
                END
            ) AS day_in_initial_period,
        
            -- EXTENSION PERIOD DAYS
            SUM(
                CASE
                    WHEN extension_days > 0
                        AND ct.calendar_date BETWEEN 
                        DATE_ADD(km.pickup_date, INTERVAL (km.days_less_extension_days) DAY)
                        AND km.return_date
                        THEN 1
                    ELSE 0
                END
            ) AS day_in_extension_period,
        
            -- REVENUE ALLOCATION FOR EACH DAY
            SUM(
                CASE
                    WHEN ct.calendar_date BETWEEN km.pickup_date AND km.return_date THEN booking_charge_aed_per_day
                    ELSE 0
                END
            ) AS booking_charge_aed_rev_allocation,
        
            SUM(
                CASE
                    WHEN ct.calendar_date BETWEEN km.pickup_date AND km.return_date THEN booking_charge_less_discount_aed_per_day
                    ELSE 0
                END
            ) AS booking_charge_less_discount_aed_rev_allocation,
        
            -- INITIAL PERIOD REVENUE ALLOCATION
            SUM(
                CASE
                    WHEN extension_days > 0
                        AND ct.calendar_date BETWEEN 
                        km.pickup_date AND DATE_ADD(km.pickup_date, INTERVAL (km.days_less_extension_days - 1) DAY)
                        THEN booking_charge_less_discount_aed_per_day
        
                    WHEN extension_days = 0
                        AND ct.calendar_date BETWEEN             
                            km.pickup_date AND
                            km.return_date
                        THEN booking_charge_less_discount_aed_per_day
        
                    ELSE 0
                END
            ) AS rev_aed_in_initial_period,
        
            -- EXTENSION PERIOD REVENUE ALLOCATION
            SUM(
                CASE
                    WHEN extension_days > 0
                        AND ct.calendar_date BETWEEN 
                        DATE_ADD(km.pickup_date, INTERVAL (km.days_less_extension_days) DAY)
                        AND km.return_date
                        THEN booking_charge_less_discount_aed_per_day
                    ELSE 0
                END
            ) AS rev_aed_in_extension_period,
        
            -- DAYS ON-RENT BY SEGMENT = VENDOR, IS_REPEAT, BOOKING_TYPE, COUNTRY
            ${repeatCode}

        FROM ezhire_key_metrics.calendar_table ct

        INNER JOIN
            key_metrics_base km
            ON ct.calendar_date >= '${booking_date}'
            AND km.return_date >= '${return_date}'
            AND ct.calendar_date >= km.booking_date
            AND ct.calendar_date <= km.return_date
            AND km.status NOT LIKE '${status}'

        GROUP BY km.created_at, ct.calendar_date, km.max_booking_datetime

        ORDER BY ct.calendar_date ASC

        -- LIMIT 10;

        -- SELECT * FROM temp;
    `;

    // console.log(baseCode);
    return baseCode;
};

module.exports = {
    generateRepeatCode
}
