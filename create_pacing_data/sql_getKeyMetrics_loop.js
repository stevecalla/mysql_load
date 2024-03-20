const fs = require('fs');
const mysql = require('mysql2');
const config = require('../utilities/config');
const { generateLogFile } = require('../utilities/generateLogFile');
const { generate_distinct_list} = require('./query_distinct_keyMetricsCore_031424');

// Function to create a Promise for managing the SSH connection and MySQL queries
function createLocalConnection() {
    return new Promise((resolve, reject) => {

        // MySQL configuration
        const mysqlConfig = config.localPacingDbConfig;

        // Create a MySQL connection pool
        const pool = mysql.createPool(mysqlConfig);

        resolve(pool);
    });
}

// Function to execute query for a each pickup_year_month
async function executeInsertQuery(pool, pickup_month_year, index) {
    return new Promise((resolve, reject) => {

        console.log(pickup_month_year);
            
        const insertQuery = `INSERT INTO pacing_base_all_calendar_dates
            SELECT
                ${index} AS grouping_id,
                pbg.pickup_month_year,
                CONCAT(pickup_month_year, '-01') AS first_day_of_month,
                LAST_DAY(CONCAT(pickup_month_year, '-01')) AS last_day_of_month,
                
                c.calendar_date AS booking_date,
                pbg.days_from_first_day_of_month,
            
                pbg.count AS count,        

                REPLACE(pbg.total_booking_charge_aed, ',', '') AS total_booking_charge_aed,
                REPLACE(pbg.total_booking_charge_less_discount_aed, ',', '') AS  total_booking_charge_less_discount_aed,
                REPLACE(pbg.running_total_booking_count, ',', '') AS running_total_booking_count,
                REPLACE(pbg.running_total_booking_charge_aed, ',', '') AS running_total_booking_charge_aed,
                REPLACE(pbg.running_total_booking_charge_less_discount_aed, ',', '') AS running_total_booking_charge_less_discount_aed

            FROM calendar_table AS c
            LEFT JOIN pacing_base_groupby AS pbg ON c.calendar_date = pbg.booking_date 
            AND pbg.pickup_month_year = '${pickup_month_year}'

            WHERE c.calendar_date > '2022-01-01'
            AND c.calendar_date <= (SELECT MAX(booking_date) FROM pacing_base_groupby WHERE pickup_month_year = '${pickup_month_year}')
            AND c.calendar_date >= (SELECT MIN(booking_date) FROM pacing_base_groupby WHERE pickup_month_year = '${pickup_month_year}')
            
            ORDER BY grouping_id, c.calendar_date ASC, pbg.days_from_first_day_of_month ASC
            -- LIMIT 10;
        `;

        pool.query(insertQuery, (queryError, results) => {
            if (queryError) {
                console.error('Error executing select query:', queryError);
                reject(queryError);
            } else {
                console.log('\nCreate table results');
                console.table(results);
                console.log('Create table results\n');
                resolve();
            }
        });

        // const addCreateAtDate = `
        // ALTER TABLE pacing_base_all_calendar_dates ADD COLUMN created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;`

        // pool.query(addCreateAtDate, (queryError, results) => {
        //     if (queryError) {
        //         console.error('Error executing select query:', queryError);
        //         reject(queryError);
        //     } else {
        //         console.log('\nCreate table results');
        //         console.table(results);
        //         console.log('Create table results\n');
        //         resolve();
        //     }
        // });

    });
}

// Function to execute query for a single date range
async function executeInsertCreatedAtQuery(pool, table) {
    return new Promise((resolve, reject) => {
        const addCreateAtDate = `
        ALTER TABLE ${table} ADD COLUMN created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;`
        console.log(addCreateAtDate);

        pool.query(addCreateAtDate, (queryError, results) => {
            if (queryError) {
                console.error('Error executing select query:', queryError);
                reject(queryError);
            } else {
                console.log('\nCreate at insert results');
                console.table(results);
                console.log('Create at insert results\n');
                resolve();
            }
        });
    });
}

async function executeDropTableQuery(pool, table) {
    return new Promise((resolve, reject) => {

        const dropTable = `DROP TABLE IF EXISTS ${table};`;

        pool.query(dropTable, (queryError, results) => {
            if (queryError) {
                console.error('Error executing select query:', queryError);
                reject(queryError);
            } else {
                console.log('\nDrop table results=');
                console.table(results);
                console.log('Drop table results\n');
                resolve();
            }
        })
    });
}

// Get distinct list of pickup_month_year
async function executeDistinctQuery(pool) {
    let list = "";
    return new Promise((resolve, reject) => {
        
        const distinctQuery = generate_distinct_list();

        pool.query(distinctQuery, (queryError, results) => {
            if (queryError) {
                console.error('Error executing select query:', queryError);
                reject(queryError);
            } else {
                // console.log('\nDistinct results=');
                // console.table(results);
                // console.log(results);
                // console.log('Distinct results\n');
                resolve(results);
            }
        })
    });
}

async function executeCreateTableQuery(pool, table) {
    return new Promise((resolve, reject) => {

        const dropTable = `CREATE TABLE IF NOT EXISTS pacing_base_all_calendar_dates (
            grouping_id INT,
            pickup_month_year VARCHAR(10),
            first_day_of_month VARCHAR(10),
            last_day_of_month DATE,
            booking_date DATE,
            days_from_first_day_of_month BIGINT,
            count INT,
            total_booking_charge_aed DECIMAL(20, 2),
            total_booking_charge_less_discount_aed DECIMAL(20, 2),
            running_total_booking_count BIGINT,
            running_total_booking_charge_aed DECIMAL(20, 2),
            running_total_booking_charge_less_discount_aed DECIMAL(20, 2)
        );`;

        pool.query(dropTable, (queryError, results) => {
            if (queryError) {
                console.error('Error executing select query:', queryError);
                reject(queryError);
            } else {
                console.log('\nCreate table results=');
                console.table(results);
                console.log('Create table results\n');
                resolve();
            }
        })
    });
}

async function executeCreateFinalDataQuery(pool) {
    return new Promise((resolve, reject) => {
            
        const query = `CREATE TABLE pacing_final_data AS
        SELECT
            -- pickup_month_year
            CASE
                WHEN pickup_month_year IS NULL THEN (
                    SELECT inner_table.pickup_month_year
                    FROM pacing_base_all_calendar_dates AS inner_table
                    WHERE inner_table.grouping_id = pacing_base_all_calendar_dates.grouping_id
                    AND inner_table.booking_date < pacing_base_all_calendar_dates.booking_date
                    AND inner_table.pickup_month_year IS NOT NULL
                    ORDER BY inner_table.booking_date DESC
                    LIMIT 1)
                ELSE pickup_month_year
                END AS pickup_month_year,
            booking_date,
            
            -- days_from_first_day_of_month
            CASE
                WHEN first_day_of_month IS NULL THEN (
                    SELECT DATEDIFF(pacing_base_all_calendar_dates.booking_date, STR_TO_DATE(inner_table.first_day_of_month  , '%Y-%m-%d'))
                    FROM pacing_base_all_calendar_dates AS inner_table
                    WHERE inner_table.grouping_id = pacing_base_all_calendar_dates.grouping_id
                    AND inner_table.booking_date < pacing_base_all_calendar_dates.booking_date
                    AND inner_table.first_day_of_month IS NOT NULL
                    ORDER BY inner_table.booking_date DESC
                    LIMIT 1)
                ELSE days_from_first_day_of_month
                END AS days_from_first_day_of_month,
            
            COALESCE(count, 0) AS count,
            COALESCE(total_booking_charge_aed, 0) AS total_booking_charge_aed,
            COALESCE(total_booking_charge_less_discount_aed, 0) AS total_booking_charge_less_discount_aed,
            
            -- running_count
            CASE
                WHEN running_total_booking_count IS NULL THEN (
                    SELECT inner_table.running_total_booking_count
                    FROM pacing_base_all_calendar_dates AS inner_table
                    WHERE inner_table.grouping_id = pacing_base_all_calendar_dates.grouping_id
                    AND inner_table.booking_date < pacing_base_all_calendar_dates.booking_date
                    AND inner_table.running_total_booking_count IS NOT NULL
                    ORDER BY inner_table.booking_date DESC
                    LIMIT 1)
                ELSE running_total_booking_count
                END AS running_count,
            
            -- running_total_booking_charge_aed
            CASE
                WHEN running_total_booking_charge_aed IS NULL THEN (
                    SELECT inner_table.running_total_booking_charge_aed
                    FROM pacing_base_all_calendar_dates AS inner_table
                    WHERE inner_table.grouping_id = pacing_base_all_calendar_dates.grouping_id
                    AND inner_table.booking_date < pacing_base_all_calendar_dates.booking_date
                    AND inner_table.running_total_booking_charge_aed IS NOT NULL
                    ORDER BY inner_table.booking_date DESC
                    LIMIT 1)
                ELSE running_total_booking_charge_aed
                END AS running_total_booking_charge_aed,
            
            -- running_total_booking_charge_less_discount_aed
            CASE
                WHEN running_total_booking_charge_less_discount_aed IS NULL THEN (
                    SELECT inner_table.running_total_booking_charge_less_discount_aed
                    FROM pacing_base_all_calendar_dates AS inner_table
                    WHERE inner_table.grouping_id = pacing_base_all_calendar_dates.grouping_id
                    AND inner_table.booking_date < pacing_base_all_calendar_dates.booking_date
                    AND inner_table.running_total_booking_charge_less_discount_aed IS NOT NULL
                    ORDER BY inner_table.booking_date DESC
                    LIMIT 1)
                ELSE running_total_booking_charge_less_discount_aed
                END AS running_total_booking_charge_less_discount_aed
                
        FROM pacing_base_all_calendar_dates;
        `;

        pool.query(query, (queryError, results) => {
            if (queryError) {
                console.error('Error executing select query:', queryError);
                reject(queryError);
            } else {
                console.log('\nCreate table results');
                console.table(results);
                console.log('Create table results\n');
                resolve();
            }
        });
    });
}

// Main function to handle SSH connection and execute queries
async function main() { 
    try {
        const pool = await createLocalConnection();

        //get distinct list
        const distinctList = await executeDistinctQuery(pool);
        console.log('results', distinctList);

        //execute drop
        await executeDropTableQuery(pool, 'pacing_base_all_calendar_dates');
        
        //execute create
        await executeCreateTableQuery(pool);

        // Execute insert for pickup_month_year; use for loop to execute sequentially
        for (let i = 0; i < distinctList.length; i++) {
            const { pickup_month_year } = distinctList[i];
            const index = i + 1;
            await executeInsertQuery(pool, pickup_month_year, index);
            console.log(`Query for ${pickup_month_year} executed successfully.`);
            generateLogFile('pacing_data', `Query for ${pickup_month_year} executed successfully.`, config.csvExportPath);
        }
    
        // Insert created at data
        await executeInsertCreatedAtQuery(pool, 'pacing_base_all_calendar_dates');
        
        // final table
        await executeDropTableQuery(pool, 'pacing_final_data;');
        await executeCreateFinalDataQuery(pool);
        await executeInsertCreatedAtQuery(pool, 'pacing_final_data');

        // generateLogFile('onrent_data', `Query for ${startDate} to ${endDate} executed successfully.`, config.csvExportPath);

        console.log('All queries executed successfully.');
        await pool.end();

    } catch (error) {
        console.error('Error:', error);
    } finally {
        // End the pool
    }
}

// Run the main function
main();