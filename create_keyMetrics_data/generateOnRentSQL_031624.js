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
        let fileName = `${variableList[i].option}`.replace(/\s+/g, '_').toLowerCase();
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
            ) AS vendor_on_rent_${fileName}${comma}
            `;
    
        repeatCode += templateCode;
    
    };

    // console.log(repeatCode);
    return(generateBaseCode(repeatCode));
};

function generateBaseCode(repeatCode) {
    
    // CREATE TEMPORARY TABLE IF NOT EXISTS temp AS
    baseCode = `
        CREATE TABLE IF NOT EXISTS temp AS
        SELECT 
            DATE_FORMAT(DATE(NOW()), '%Y-%m-%d %H:%i:%s') AS created_at,
            DATE_FORMAT(DATE(ct.calendar_date), '%Y-%m-%d') AS calendar_date,
            YEAR(ct.calendar_date) AS year,
            QUARTER(ct.calendar_date) AS quarter,
            MONTH(ct.calendar_date) AS month,
            WEEK(ct.calendar_date) AS week,
            DAY(ct.calendar_date) AS day,
            
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
        
            -- REVENUE ALLOCATION
            SUM(
                CASE
                    -- between is inclusive of pickup and return date
                    WHEN ct.calendar_date BETWEEN km.pickup_date AND km.return_date THEN booking_charge_aed_per_day
                    ELSE 0
                END
            ) AS booking_charge_aed_rev_allocation,
        
            SUM(
                CASE
                    -- between is inclusive of pickup and return date
                    WHEN ct.calendar_date BETWEEN km.pickup_date AND km.return_date THEN booking_charge_less_discount_aed_per_day
                    ELSE 0
                END
            ) AS booking_charge_Less_discount_aed_rev_allocation,
            
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

        GROUP BY ct.calendar_date

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
