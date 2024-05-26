const fs = require('fs');
const mysql = require('mysql2');
const dotenv = require('dotenv');
dotenv.config({ path: "../../.env" }); // adding the path to read the .env file

const { localUserDbConfig, csvExportPath} = require('../../utilities/config');
const { createLocalDBConnection } = require('../../utilities/connectionLocalDB');
const { generateLogFile } = require('../../utilities/generateLogFile');
const { getCurrentDateTime } = require('../../utilities/getCurrentDate');

const { get_distinct } = require('./sql_get_distinct_fields_loop');
const { generateRepeatCode } = require('./generateOnRentSQL_031624');

const { query_create_cohort_base_data } = require('./query_create_cohort_base_data');
const { query_insert_cohort_base_data } = require('./query_insert_cohort_base_data');

async function execute_drop_table_query(pool, table) {
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

async function execute_insert_createdAt_query(pool, table) {
    return new Promise((resolve, reject) => {

        const startTime = performance.now();

        const addCreateAtDate = `ALTER TABLE ${table} ADD COLUMN created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;`;
        
        console.log(addCreateAtDate);

        pool.query(addCreateAtDate, (queryError, results) => {
            const endTime = performance.now();
            const elapsedTime = ((endTime - startTime) / 1_000).toFixed(2); //convert ms to sec

            if (queryError) {
                console.error('Error executing select query:', queryError);
                reject(queryError);
            } else {
                console.log('\nCreate at insert results');
                console.table(results);
                console.log('Create at insert results\n');
                console.log(`Query results: ${results.info}, Elapsed Time: ${elapsedTime} sec\n`);
                resolve();
            }
        });
    });
}

// STEP 4.1: CREATE COHORT BASE DATA TABLE
async function execute_create_base_data_query(pool) {
    return new Promise((resolve, reject) => {

        const startTime = performance.now();

        const query = query_create_cohort_base_data;

        pool.query(query, (queryError, results) => {
            const endTime = performance.now();
            const elapsedTime = ((endTime - startTime) / 1_000).toFixed(2); //convert ms to sec

            if (queryError) {
                console.error('Error executing select query:', queryError);
                reject(queryError);
            } else {
                console.log('\nCreate table results');
                console.table(results);
                console.log('Create table results\n');
                console.log(`Query results: ${results.info}, Elapsed Time: ${elapsedTime} sec\n`);
                resolve();
            }
        });
    });
}

// STEP 4.2: INSERT COHORT BASE DATA
async function execute_insert_base_data_query(pool, table) {
    return new Promise((resolve, reject) => {

        const startTime = performance.now();

        const query = query_insert_cohort_base_data;

        pool.query(query, (queryError, results) => {
            const endTime = performance.now();
            const elapsedTime = ((endTime - startTime) / 1_000).toFixed(2); //convert ms to sec
            
            if (queryError) {
                console.error('Error executing select query:', queryError);
                reject(queryError);
            } else {
                console.log('\nInsert base data\n');
                console.table(results);
                console.log('Insert base data\n');
                console.log(`Query results: ${results.info}, Elapsed Time: ${elapsedTime} sec\n`);
                resolve();
            }
        });
    });
}

// STEP 4.4: CREATE USER COHORT DATA
async function execute_create_user_data_cohort_stats(pool, distinctList) {

    const query = await generateRepeatCode(distinctList);

    return new Promise((resolve, reject) => {

        const startTime = performance.now();

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
async function execute_create_cohort_stats() {
    let pool;
    const startTime = performance.now();

    try {
        pool = await createLocalDBConnection(localUserDbConfig);

        // STEP 4.0: CREATE CALENDAR TABLE - ONLY NECESSARY IF CALENDAR NEEDS REVISION

        // STEP 4.1: CREATE COHORT BASE DATA TABLE
        console.log(`\nSTEP 4.1: CREATE COHORT BASE DATA TABLE`);
        console.log(getCurrentDateTime());
        await execute_drop_table_query(pool, 'user_data_cohort_base');
        await execute_create_base_data_query(pool);
        
        // STEP 4.2: INSERT COHORT BASE DATA
        console.log(`\nSTEP 4.2: INSERT COHORT BASE DATA`);
        console.log(getCurrentDateTime());
        await execute_insert_base_data_query(pool);
        await execute_insert_createdAt_query(pool, 'user_data_cohort_base'); 

        // STEP 4.3: GET DISTINCT LIST FOR VENDOR, REPEAT, COUNTRY, BOOKING TYPE
        console.log(`\nSTEP 4.3: GET DISTINCT LIST FOR VENDOR, REPEAT, COUNTRY, BOOKING TYPE`);
        console.log(getCurrentDateTime());
        const distinctList = await get_distinct();

        // STEP 4.4: CREATE USER COHORT DATA
        console.log(`\nSTEP 4.4: CREATE USER COHORT DATA`);
        console.log(getCurrentDateTime());
        await execute_drop_table_query(pool, 'user_data_cohort_stats');
        await execute_create_user_data_cohort_stats(pool, distinctList);

        generateLogFile('loading_user_data', `Query for CREATE USER COHORT DATA executed successfully.`, csvExportPath);
        
        console.log('All queries executed successfully.');
        
    } catch (error) {
        console.error('Error:', error);
        generateLogFile('loading_user_data', `Error loading user data: ${error}`, csvExportPath);

    } finally {
        // CLOSE CONNECTION/POOL
        await pool.end(err => {
            if (err) {
              console.error('Error closing connection pool:', err.message);
            } else {
              console.log('Connection pool closed successfully.');
            }
        });

        // LOG RESULTS
        const endTime = performance.now();
        const elapsedTime = ((endTime - startTime) / 1_000).toFixed(2); //convert ms to sec
        console.log(`\nAll create key metrics queries executed successfully. Elapsed Time: ${elapsedTime ? elapsedTime : "Opps error getting time"} sec\n`);

        return elapsedTime;
    }
}

// Run the main function
// execute_create_cohort_stats();

module.exports = {
    execute_create_cohort_stats,
}
