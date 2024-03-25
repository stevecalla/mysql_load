const fs = require('fs');
const mysql = require('mysql2');
const { localKeyMetricsDbConfig } = require('../utilities/config');
const { createLocalDBConnection } = require('../utilities/connectionLocalDB');
const { get_distinct } = require('./sql_getDistinct_fields_loop');
const { generateRepeatCode } = require('./generateOnRentSQL_031624');
const { generateLogFile } = require('../utilities/generateLogFile');

async function executeDropTableQuery(pool, table) {
    return new Promise((resolve, reject) => {

        const dropTable = `DROP TABLE IF EXISTS ${table};`;

        pool.query(dropTable, (queryError, results) => {
            if (queryError) {
                console.error('Error executing select query:', queryError);
                reject(queryError);
            } else {
                console.log(`\nDrop table results= ${table}`);
                console.table(results);
                console.log('Drop table results\n');
                resolve();
            }
        })
    });
}

async function executeCreateBaseDataQuery(pool) {
    return new Promise((resolve, reject) => {
            
        const query = `
        -- Step 1: Create the table structure (assuming the structure of booking_data is known)
        CREATE TABLE IF NOT EXISTS key_metrics_base (
            id INT PRIMARY KEY AUTO_INCREMENT,
            booking_id INT,
            status VARCHAR(64),
            booking_type VARCHAR(64),
            vendor VARCHAR(64),
            is_repeat VARCHAR(64),
            country VARCHAR(64),
        
            -- BOOKING DATE FIELD
            booking_date DATE,
        
            -- PICKUP DATE FIELDS
            pickup_date DATE,
            pickup_datetime DATETIME,
            pickup_time TIME AS (TIME(pickup_datetime)),
        
            return_date DATE,
            return_datetime DATETIME,
            return_time TIME AS (TIME(return_datetime)),
        
            -- CONSTANT MINUTES IN A DAY
            total_minutes_in_day INT DEFAULT (24 * 60),
        
            -- DAYS CALCULATION
            minutes_rented DECIMAL(20, 4) AS (TIMESTAMPDIFF(MINUTE, pickup_datetime, return_datetime)),
            days_rented DECIMAL(10, 4) AS (TIMESTAMPDIFF(DAY, pickup_datetime, return_datetime)),
            days_less_extension_days DECIMAL(10, 4) AS ((TIMESTAMPDIFF(DAY, pickup_datetime, return_datetime)) - extension_days),
            extension_days DECIMAL(10, 4),
        
            -- REVENUE CALCULATION
            booking_charge_aed DOUBLE,
            booking_charge_less_discount_aed DOUBLE,
        
            booking_charge_less_discount_extension_aed DOUBLE,
            extension_charge_aed DOUBLE,
        
            -- BOOKING CHARGE AED PER DAY
            booking_charge_aed_per_day DOUBLE AS (
                CASE
                    WHEN pickup_date = return_date THEN booking_charge_aed
                    WHEN pickup_date <> return_date AND days_rented <= 2 THEN booking_charge_aed / 2
                    -- ELSE booking_charge_aed / days_rented
                    
                    WHEN days_rented > 0 THEN booking_charge_aed / days_rented
                    ELSE 0            
                END
            ),
        
            -- BOOKING CHARGE LESS DISCOUNT AED PER DAY
            booking_charge_less_discount_aed_per_day DOUBLE AS (
                CASE
                    WHEN pickup_date = return_date THEN booking_charge_less_discount_aed
                    WHEN pickup_date <> return_date AND days_rented <= 2 THEN booking_charge_less_discount_aed / 2      
                    WHEN days_rented > 0 THEN booking_charge_less_discount_aed / days_rented
                    ELSE 0
                END
            ),
        
            -- BOOKING CHARGE LESS DISCOUNT LESS EXTENSION AED PER DAY
            booking_charge_less_discount_extension_aed_per_day DOUBLE AS (
                CASE
                    WHEN pickup_date = return_date THEN booking_charge_less_discount_extension_aed
                    WHEN pickup_date <> return_date AND days_less_extension_days <= 2 THEN booking_charge_less_discount_extension_aed / 2
                    WHEN days_less_extension_days > 0 THEN booking_charge_less_discount_extension_aed / days_less_extension_days
                    ELSE 0
                END
            ),
        
            -- EXTENSION CHARGE AED PER DAY CALC
            extension_charge_aed_per_day DOUBLE AS (
                CASE
                    WHEN pickup_date = return_date THEN extension_charge_aed
                    WHEN pickup_date <> return_date AND extension_days <= 2 THEN extension_charge_aed / 2
                    WHEN extension_days > 0 THEN extension_charge_aed / extension_days
                    ELSE 0
                END
            ),
        
            -- PICKUP MINUTE FRACTION CALC
            pickup_hours_to_midnight INT AS (HOUR(TIMEDIFF('24:00:00', pickup_time))) VIRTUAL,
            pickup_minutes_to_midnight INT AS (MINUTE(TIMEDIFF('24:00:00', pickup_time))) VIRTUAL,
            pickup_total_minutes_to_midnight INT AS (pickup_hours_to_midnight * 60 + pickup_minutes_to_midnight) VIRTUAL,
            pickup_fraction_of_day DECIMAL(5, 4) AS (pickup_total_minutes_to_midnight / total_minutes_in_day ) STORED,
            
            -- RETURN MINUTE FRACTION CALC
            return_hours_to_midnight INT AS (HOUR(return_time)) VIRTUAL,
            return_minutes_to_midnight INT AS (MINUTE(return_time)) VIRTUAL,
            return_total_minutes_to_midnight INT AS (return_hours_to_midnight * 60 + return_minutes_to_midnight) VIRTUAL,
            return_fraction_of_day DECIMAL(5, 4) AS (return_total_minutes_to_midnight / total_minutes_in_day ) STORED,
        
            -- Create indexes on pickup_date, return_date, and status in key_metrics_base
            INDEX idx_pickup_date (pickup_date),
            INDEX idx_return_date (return_date),
            INDEX idx_status (status),
            INDEX idx_pickup_return_date (pickup_date, return_date)
        );
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

async function executeInsertBaseDataQuery(pool, table) {
    return new Promise((resolve, reject) => {

        const query = `
        -- Step 2: Insert data from ezhire_booking_data.booking_data into key_metrics table
        INSERT INTO key_metrics_base (booking_id, status, booking_type, vendor, is_repeat, country, booking_date, pickup_date, pickup_datetime, return_date, return_datetime, extension_days, booking_charge_aed, booking_charge_less_discount_aed, extension_charge_aed, booking_charge_less_discount_extension_aed)
        
        SELECT booking_id, status, booking_type, marketplace_or_dispatch AS vendor, repeated_user AS is_repeat, deliver_country AS country, booking_date, pickup_date, pickup_datetime, return_date, return_datetime, extension_days, booking_charge_aed, booking_charge_less_discount_aed, extension_charge_aed, booking_charge_less_discount_extension_aed
        
        FROM ezhire_booking_data.booking_data;
        `;


        pool.query(query, (queryError, results) => {
            if (queryError) {
                console.error('Error executing select query:', queryError);
                reject(queryError);
            } else {
                console.log('\nInsert base data\n');
                console.table(results);
                console.log('Insert base data\n');
                resolve();
            }
        });
    });
}

// Function to execute query for a single date range
async function executeQuery(pool, distinctList) {

    //create sql query
    const query = await generateRepeatCode(distinctList);

    return new Promise((resolve, reject) => {

        const startTime = performance.now();

        const dropTable = `DROP TABLE IF EXISTS key_metrics_data;`;

        pool.query(dropTable, (queryError, results) => {
            if (queryError) {
                console.error('Error executing select query:', queryError);
                reject(queryError);
            } else {
                console.log('\nDrop table results=');
                console.table( results);
            }
        })

        pool.query(query, (queryError, results) => {
            const endTime = performance.now();
            const elapsedTime = ((endTime - startTime) / 1_000).toFixed(2); //convert ms to sec

            if (queryError) {
                console.error('Error executing select query:', queryError);
                reject(queryError);
            } else {
                console.log('\nCreate table results');
                console.table( results);
                console.log('Create table results\n');
                console.log(`Query results length: ${results.length}, Elapsed Time: ${elapsedTime} sec`);
                resolve();
            }
        });
    });
}

// Main function to handle SSH connection and execute queries
async function execute_create_key_metrics() {
    try {
        const startTime = performance.now();
        
        const pool = await createLocalDBConnection(localKeyMetricsDbConfig);

        //STEP 1: CREATE CALENDAR TABLE - ONLY NECESSARY IF CALENDAR NEEDS REVISION

        //STEP 2: CREATE BASE DATA
        await executeDropTableQuery(pool, 'key_metrics_base');
        await executeCreateBaseDataQuery(pool);
        await executeInsertBaseDataQuery(pool);
        await executeInsertCreatedAtQuery(pool, 'key_metrics_base'); 

        //STEP 3: GET DISTINCT LIST FOR VENDOR, REPEAT, COUNTRY, BOOKING TYPE
        const distinctList = await get_distinct();

        //STEP 4: GET ALL DATA -- send distinct list to the execute function
        await executeQuery(pool, distinctList);

        // generateLogFile('onrent_data', `Query for ${startDate} to ${endDate} executed successfully.`, csvExportPath);
        
        await pool.end(err => {
            if (err) {
                console.error('Error closing connection pool:', err.message);
            } else {
                console.log('Connection pool closed successfully.');
            }
        });
        
        console.log('All queries executed successfully.');

        // LOGS
        const endTime = performance.now();
        const elapsedTime = ((endTime - startTime) / 1_000).toFixed(2); //convert ms to sec
        // MOVED THE MESSAGE BELOW TO THE BOOKING_JOB_032024 PROCESS
        // console.log(`\nAll create key metrics queries executed successfully. Elapsed Time: ${elapsedTime ? elapsedTime : "Opps error getting time"} sec\n`);
        return elapsedTime;
        
    } catch (error) {
        console.error('Error:', error);
    } finally {
        // End the pool
    }
}

// Run the main function
// execute_create_key_metrics();

module.exports = {
    execute_create_key_metrics,
}
