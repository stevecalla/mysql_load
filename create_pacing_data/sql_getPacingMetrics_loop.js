const fs = require('fs');
const mysql = require('mysql2');
const { localPacingDbConfig, csvExportPath } = require('../utilities/config');
const { createLocalDBConnection } = require('../utilities/connectionLocalDB');

const { generate_distinct_list } = require('./query_distinct_pickup_month_year_031424');
const { query_pacing_groupby_optimized } = require('./query_create_pacing_groupby_optimized');

const { getCurrentDateTime } = require('../utilities/getCurrentDate');

const { generateLogFile } = require('../utilities/generateLogFile');
// const { get_spinner, clear_spinner } = require('../utilities/spinner');

// STEP #6 - ADD CREATED AT DATE
async function executeInsertCreatedAtQuery(pool, table) {
    return new Promise((resolve, reject) => {

        const startTime = performance.now();

        const addCreateAtDate = `ALTER TABLE ${table} ADD COLUMN created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;`;

        pool.query(addCreateAtDate, (queryError, results) => {
            const endTime = performance.now();
            const elapsedTime = ((endTime - startTime) / 1_000).toFixed(2); //convert ms to sec

            if (queryError) {
                console.error('Error executing select query:', queryError);
                reject(queryError);
            } else {
                console.log('\nInsert "created at" date');
                console.table(results);
                console.log(`Query results: ${results.info}, Elapsed Time: ${elapsedTime} sec\n`);

                resolve();
            }
        });
    });
}

async function executeDropTableQuery(pool, table) {
    return new Promise((resolve, reject) => {

        const startTime = performance.now();

        const dropTable = `DROP TABLE IF EXISTS ${table};`;

        pool.query(dropTable, (queryError, results) => {
            const endTime = performance.now();
            const elapsedTime = ((endTime - startTime) / 1_000).toFixed(2); //convert ms to sec

            if (queryError) {
                console.error('Error executing select query:', queryError);
                reject(queryError);
            } else {
                console.log(`\nDrop table results= ${table}`);
                console.table(results);
                console.log(`Query results: ${results.info}, Elapsed Time: ${elapsedTime} sec\n`);
                resolve();
            }
        })
    });
}

// Get distinct list of pickup_month_year
async function executeDistinctQuery(pool) {
    return new Promise((resolve, reject) => {

        const startTime = performance.now();
        
        const distinctQuery = generate_distinct_list();

        pool.query(distinctQuery, (queryError, results) => {
            const endTime = performance.now();
            const elapsedTime = ((endTime - startTime) / 1_000).toFixed(2); //convert ms to sec

            if (queryError) {
                console.error('Error executing select query:', queryError);
                reject(queryError);
            } else {
                console.log(`Query results: ${results.info}, Elapsed Time: ${elapsedTime} sec\n`);
                resolve(results);
            }
        })
    });
}

// STEP #1 - CREATE BASE DATA
async function executeCreateBaseDataQuery(pool) {
    return new Promise((resolve, reject) => {

        const startTime = performance.now();
  
        const query = `
        -- CREATE PACING BASE STATS
        CREATE TABLE pacing_base AS 
        SELECT 
            booking_id,
            booking_date,
            max_booking_datetime, -- ADDED
            DATE_FORMAT(pickup_date, '%Y-%m-01') AS pickup_first_day_of_month,
            TIMESTAMPDIFF(DAY, DATE_FORMAT(pickup_date, '%Y-%m-01'), booking_date) AS days_from_first_day_of_month,
        
            CONCAT(pickup_year, "-", LPAD(pickup_month, 2, '0')) AS pickup_month_year,
            pickup_date,
    
            1 AS count,
            booking_charge_aed,
            booking_charge_less_discount_aed,
            booking_charge_less_discount_extension_aed,
            extension_charge_aed
        
        FROM ezhire_booking_data.booking_data 
        WHERE status NOT LIKE '%Cancel%'
        AND pickup_year IN (2023, 2024, 2025)
        ORDER BY booking_date ASC, pickup_date ASC;
        -- LIMIT 10;
        `;

        pool.query(query, (queryError, results) => {
            const endTime = performance.now();
            const elapsedTime = ((endTime - startTime) / 1_000).toFixed(2); //convert ms to sec

            if (queryError) {
                console.error('Error executing select query:', queryError);
                reject(queryError);
            } else {
                
                console.log('\nCreate table results = CREATE BASE DATA');
                console.table(results);
                console.log(`Query results: ${results.info}, Elapsed Time: ${elapsedTime} sec\n`);
                resolve();
            }
        });
    });
}

// STEP #2 - ROLLUP BASE DATA
async function executeCreateGroupByDataQuery(pool) {
    return new Promise((resolve, reject) => {

        const startTime = performance.now();

        const query = query_pacing_groupby_optimized();

        pool.query(query, (queryError, results) => {
            const endTime = performance.now();
            const elapsedTime = ((endTime - startTime) / 1_000).toFixed(2); //convert ms to sec

            if (queryError) {
                console.error('Error executing select query:', queryError);
                reject(queryError);
            } else {
                console.log('\nCreate table results = ROLLUP BASE DATA');
                console.table(results);
                console.log(`Query results: ${results.info}, Elapsed Time: ${elapsedTime} sec\n`);
                resolve();
            }
        });
    });
}

// STEP #3a - INSERT MISSING DATES
async function executeCreateTableQuery(pool) {
    return new Promise((resolve, reject) => {

        const startTime = performance.now();

        const query = `CREATE TABLE IF NOT EXISTS pacing_base_all_calendar_dates (
            grouping_id INT,
            max_booking_datetime DATETIME,
    
            -- CALC IS_BEFORE_TODAY
            is_before_today VARCHAR(3),

            pickup_month_year VARCHAR(10),
            first_day_of_month VARCHAR(10),
            last_day_of_month DATE,
            booking_date DATE,
            days_from_first_day_of_month BIGINT,
    
            count INT,
            total_booking_charge_aed DECIMAL(20, 2),
            total_booking_charge_less_discount_aed DECIMAL(20, 2),
            total_booking_charge_less_discount_extension_aed DECIMAL(20, 2),
            total_extension_charge_aed DECIMAL(20, 2),
    
            running_total_booking_count BIGINT,
            running_total_booking_charge_aed DECIMAL(20, 2),
            running_total_booking_charge_less_discount_aed DECIMAL(20, 2),
            running_total_booking_charge_less_discount_extension_aed DECIMAL(20, 2),
            running_total_extension_charge_aed DECIMAL(20, 2)
        );`;

        pool.query(query, (queryError, results) => {
            const endTime = performance.now();
            const elapsedTime = ((endTime - startTime) / 1_000).toFixed(2); //convert ms to sec

            if (queryError) {
                console.error('Error executing select query:', queryError);
                reject(queryError);
            } else {
                console.log('\nCreate table results= INSERT MISSING DATES 3a');
                console.table(results);
                console.log(`Query results: ${results.info}, Elapsed Time: ${elapsedTime} sec\n`);
                resolve();
            }
        })
    });
}

// STEP #3b - Function to execute query for a each pickup_year_month
async function executeInsertQuery(pool, pickup_month_year, index) {
    return new Promise((resolve, reject) => {

        const startTime = performance.now();
            
        const insertQuery = `INSERT INTO pacing_base_all_calendar_dates
            SELECT
                ${index} AS grouping_id,
                pbg.max_booking_datetime,

                -- CALC IS_BEFORE_TODAY
                CASE
                    WHEN days_from_first_day_of_month IS NULL THEN NULL
                    WHEN days_from_first_day_of_month <= DATE_FORMAT(max_booking_datetime, '%d') + 2 THEN "yes"
                    ELSE "no"
                END AS is_before_today,

                pbg.pickup_month_year,
                CONCAT(pickup_month_year, '-01') AS first_day_of_month,
                LAST_DAY(CONCAT(pickup_month_year, '-01')) AS last_day_of_month,
                
                c.calendar_date AS booking_date,
                pbg.days_from_first_day_of_month,

                pbg.count AS count,
                REPLACE(pbg.total_booking_charge_aed, ',', '') AS total_booking_charge_aed,
                REPLACE(pbg.total_booking_charge_less_discount_aed, ',', '') AS  total_booking_charge_less_discount_aed,
                REPLACE(pbg.total_booking_charge_less_discount_extension_aed, ',', '') AS  total_booking_charge_less_discount_extension_aed,
                REPLACE(pbg.total_extension_charge_aed, ',', '') AS  total_extension_charge_aed,

                REPLACE(pbg.running_total_booking_count, ',', '') AS running_total_booking_count,
                REPLACE(pbg.running_total_booking_charge_aed, ',', '') AS running_total_booking_charge_aed,
                REPLACE(pbg.running_total_booking_charge_less_discount_aed, ',', '') AS running_total_booking_charge_less_discount_aed,
                REPLACE(pbg.running_total_booking_charge_less_discount_extension_aed, ',', '') AS running_total_booking_charge_less_discount_extension_aed,
                REPLACE(pbg.running_total_extension_charge_aed, ',', '') AS running_total_extension_charge_aed

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
            const endTime = performance.now();
            const elapsedTime = ((endTime - startTime) / 1_000).toFixed(2); //convert ms to sec

            if (queryError) {
                console.error('Error executing select query:', queryError);
                reject(queryError);
            } else {
                console.log(`\nCreate table results 3b = ${pickup_month_year}`);
                console.log(`Query results: ${results.info}, Elapsed Time: ${elapsedTime} sec\n`);
                console.log(`Query for ${pickup_month_year} executed successfully.`);
                resolve();
            }
        });
    });
}

// STEP #3c - INSERT MISSING DATES
async function executeCreateFinalDataQuery(pool) {
    return new Promise((resolve, reject) => {

        const startTime = performance.now();
            
        const query = `CREATE TABLE pacing_final_data AS
        SELECT
            CASE
                WHEN max_booking_datetime IS NULL THEN (
                    SELECT inner_table.max_booking_datetime
                    FROM pacing_base_all_calendar_dates AS inner_table
                    WHERE inner_table.grouping_id = pacing_base_all_calendar_dates.grouping_id
                    AND inner_table.booking_date < pacing_base_all_calendar_dates.booking_date
                    AND inner_table.max_booking_datetime IS NOT NULL
                    ORDER BY inner_table.booking_date DESC
                    LIMIT 1)
                ELSE max_booking_datetime
            END AS max_booking_datetime, -- ADDED

            CASE
                WHEN is_before_today IS NULL THEN (
                    SELECT inner_table.is_before_today
                    FROM pacing_base_all_calendar_dates AS inner_table
                    WHERE inner_table.grouping_id = pacing_base_all_calendar_dates.grouping_id
                    AND inner_table.booking_date < pacing_base_all_calendar_dates.booking_date
                    AND inner_table.is_before_today IS NOT NULL
                    ORDER BY inner_table.booking_date DESC
                    LIMIT 1)
                ELSE is_before_today
            END AS is_before_today, -- ADDED

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

            booking_date, -- populated for all rows
            
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
            COALESCE(total_booking_charge_less_discount_extension_aed, 0) AS total_booking_charge_less_discount_extension_aed,
            COALESCE(total_extension_charge_aed, 0) AS total_extension_charge_aed,
        
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
            END AS running_total_booking_charge_less_discount_aed,
        
            -- running_total_booking_charge_less_discount_extension_aed
            CASE
                WHEN running_total_booking_charge_less_discount_extension_aed IS NULL THEN (
                    SELECT inner_table.running_total_booking_charge_less_discount_extension_aed
                    FROM pacing_base_all_calendar_dates AS inner_table
                    WHERE inner_table.grouping_id = pacing_base_all_calendar_dates.grouping_id
                    AND inner_table.booking_date < pacing_base_all_calendar_dates.booking_date
                    AND inner_table.running_total_booking_charge_less_discount_extension_aed IS NOT NULL
                    ORDER BY inner_table.booking_date DESC
                    LIMIT 1)
                ELSE running_total_booking_charge_less_discount_extension_aed
            END AS running_total_booking_charge_less_discount_extension_aed,
        
            -- running_total_extension_charge_aed
            CASE
                WHEN running_total_extension_charge_aed IS NULL THEN (
                    SELECT inner_table.running_total_extension_charge_aed
                    FROM pacing_base_all_calendar_dates AS inner_table
                    WHERE inner_table.grouping_id = pacing_base_all_calendar_dates.grouping_id
                    AND inner_table.booking_date < pacing_base_all_calendar_dates.booking_date
                    AND inner_table.running_total_extension_charge_aed IS NOT NULL
                    ORDER BY inner_table.booking_date DESC
                    LIMIT 1)
                ELSE running_total_extension_charge_aed
            END AS running_total_extension_charge_aed
                
        FROM pacing_base_all_calendar_dates;
        `;

        pool.query(query, (queryError, results) => {
            const endTime = performance.now();
            const elapsedTime = ((endTime - startTime) / 1_000).toFixed(2); //convert ms to sec

            if (queryError) {
                console.error('Error executing select query:', queryError);
                reject(queryError);
            } else {
                console.log('\nCreate table results = INSERT MISSING DATES 3c');
                console.table(results);
                console.log(`Query results: ${results.info}, Elapsed Time: ${elapsedTime} sec\n`);
                resolve();
            }
        });
    });
}

// Main function to handle SSH connection and execute queries
async function execute_create_pacing_metrics() { 
    try {

        const startTime = performance.now();
        const pool = await createLocalDBConnection(localPacingDbConfig);
        // console.log(pool.config.connectionConfig.user, pool.config.connectionConfig.database);

        //STEP 4.1: CREATE CALENDAR TABLE - ONLY NECESSARY IF CALENDAR NEEDS REVISION
        console.log(`\nSTEP 4.1: CREATE CALENDAR TABLE - ONLY NECESSARY IF CALENDAR NEEDS REVISION`);
        console.log(getCurrentDateTime());

        //STEP 4.2: CREATE BASE DATA
        console.log(`STEP 4.2: CREATE BASE DATA`);
        console.log(getCurrentDateTime());
        await executeDropTableQuery(pool, 'pacing_base;');
        await executeCreateBaseDataQuery(pool);
        await executeInsertCreatedAtQuery(pool, 'pacing_base');  
        
        //STEP 4.3: CREATE ROLLUP RUNNING TOTALS GROUP BY DATA
        console.log(`STEP 4.3: CREATE ROLLUP RUNNING TOTALS GROUP BY DATA`);
        console.log(getCurrentDateTime());
        await executeDropTableQuery(pool, 'pacing_base_groupby;');
        console.log(`Executing create group by / rollup by date`);
        await executeCreateGroupByDataQuery(pool);
        await executeInsertCreatedAtQuery(pool, 'pacing_base_groupby');   

        //STEP 4.4: ADD MISSING CALENDAR DATES TO EACH MONTH
        console.log(`STEP 4.4: ADD MISSING CALENDAR DATES TO EACH MONTH`);
        console.log(getCurrentDateTime());
        const distinctList = await executeDistinctQuery(pool); // get list of pickup_year_month

        await executeDropTableQuery(pool, 'pacing_base_all_calendar_dates'); // drop prior table
        await executeCreateTableQuery(pool); // create table

        for (let i = 0; i < distinctList.length; i++) { // insert data into table; loop ensure sequential process
            const { pickup_month_year } = distinctList[i];
            const index = i + 1;

            await executeInsertQuery(pool, pickup_month_year, index);

            generateLogFile('pacing_data', `Query for ${pickup_month_year} executed successfully.`, csvExportPath);
        }
        
        //STEP 5: ROLLUP THE DATA BY PICKUP_MONTH_YEAR
        console.log(`STEP 5: ROLLUP THE DATA BY PICKUP_MONTH_YEAR`);
        console.log(getCurrentDateTime());
        await executeDropTableQuery(pool, 'pacing_final_data;');
        await executeCreateFinalDataQuery(pool);

        //STEP 6: ADD CREATED AT DATE
        await executeInsertCreatedAtQuery(pool, 'pacing_final_data');

        console.log('All queries executed successfully.');

        await pool.end(err => {
          if (err) {
            console.error('Error closing connection pool:', err.message);
          } else {
            console.log('Connection pool closed successfully.');
          }
        });

        const endTime = performance.now();
        const elapsedTime = ((endTime - startTime) / 1_000).toFixed(2); //convert ms to sec
        // MOVED THE MESSAGE BELOW TO THE BOOKING_JOB_032024 PROCESS
        // console.log(`\nAll create pacing data queries executed successfully. Elapsed Time: ${elapsedTime ? elapsedTime : "Opps error getting time"} sec\n`);
        return elapsedTime;

    } catch (error) {
        console.error('Error:', error);
    } finally {
        // End the pool
    }
}

// Run the main function
// execute_create_pacing_metrics();

module.exports = {
    execute_create_pacing_metrics,
}