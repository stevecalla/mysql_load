const fs = require('fs');
const mysql = require('mysql2');
const dotenv = require('dotenv');
dotenv.config({ path: "../../.env" }); // adding the path ensures each folder will read the .env file as necessary

const { localUserDbConfig, csvExportPath } = require('../../utilities/config');
const { createLocalDBConnection } = require('../../utilities/connectionLocalDB');
const { getCurrentDateTime } = require('../../utilities/getCurrentDate');
const { generateLogFile } = require('../../utilities/generateLogFile');

const { query_combine_user_and_booking_data,
    drop_idx_user_ptr_id_dates,
    drop_idx_user_ptr_id_return_date,
    drop_idx_user_ptr_id_status,
    create_idx_user_ptr_id_dates,
    create_idx_user_ptr_id_return_date,
    create_idx_user_ptr_id_status,
} = require('./query_combine_user_and_booking_data');
const { query_create_key_metrics_rollup } = require('./query_create_key_metrics_rollup');
const { query_create_user_profile_data } = require('./query_create_user_profile_data');

async function execute_insert_createdAt_query(pool, table) {
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

async function execute_drop_table_query(pool, table) {
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

// STEP #3.1 - CREATE USER BASE DATA
async function execute_create_combined_data_query(pool) {
    return new Promise((resolve, reject) => {

        const startTime = performance.now();
  
        const query = query_combine_user_and_booking_data;

        pool.query(query, (queryError, results) => {
            const endTime = performance.now();
            const elapsedTime = ((endTime - startTime) / 1_000).toFixed(2); //convert ms to sec

            if (queryError) {
                console.error('Error executing select query:', queryError);
                reject(queryError);
            } else {
                console.log('\nCreate user base data');
                console.table(results);
                console.log(`Query results: ${results.info}, Elapsed Time: ${elapsedTime} sec\n`);
                resolve();
            }
        });
    });
}

async function execute_create_combined_data_index(pool, indexQuery) {
    return new Promise((resolve, reject) => {

        const startTime = performance.now();
  
        const query = indexQuery;

        pool.query(query, (queryError, results) => {
            const endTime = performance.now();
            const elapsedTime = ((endTime - startTime) / 1_000).toFixed(2); //convert ms to sec

            if (queryError) {
                console.error('Error executing drop/create index:', queryError);
                reject(queryError);
            } else {
                console.log('\nCreate index');
                console.table(results);
                console.log(`Query results: ${results.info}, Elapsed Time: ${elapsedTime} sec\n`);
                resolve();
            }
        });
    });
}

// STEP #3.2 - CREATE USER KEY STATS ROLLUP DATA
async function execute_create_rollup_data_query(pool) {
    return new Promise((resolve, reject) => {

        const startTime = performance.now();

        const query = query_create_key_metrics_rollup;

        pool.query(query, (queryError, results) => {
            const endTime = performance.now();
            const elapsedTime = ((endTime - startTime) / 1_000).toFixed(2); //convert ms to sec

            if (queryError) {
                console.error('Error executing select query:', queryError);
                reject(queryError);
            } else {
                console.log('\nCreate user data key stats rollup');
                console.table(results);
                console.log(`Query results: ${results.info}, Elapsed Time: ${elapsedTime} sec\n`);
                resolve();
            }
        });
    });
}

// STEP #3.3 - CREATE PROFILE DATA
async function execute_create_user_profile_data_query(pool) {
    return new Promise((resolve, reject) => {

        const startTime = performance.now();

        const query = query_create_user_profile_data;

        pool.query(query, (queryError, results) => {
            const endTime = performance.now();
            const elapsedTime = ((endTime - startTime) / 1_000).toFixed(2); //convert ms to sec

            if (queryError) {
                console.error('Error executing select query:', queryError);
                reject(queryError);
            } else {
                console.log('\nCreate user profile data');
                console.table(results);
                console.log(`Query results: ${results.info}, Elapsed Time: ${elapsedTime} sec\n`);
                resolve();
            }
        });
    });
}

// Main function to handle SSH connection and execute queries
async function execute_create_user_data() { 
    let pool;
    const startTime = performance.now();

    try {
        pool = await createLocalDBConnection(localUserDbConfig);
        // console.log(pool.config.connectionConfig.user, pool.config.connectionConfig.database);

        // STEP 3.1: COMBINE USER AND BOOKING DATA
        console.log(`STEP 3.1: COMBINE USER AND BOOKING DATA`);
        console.log(getCurrentDateTime());

        await execute_drop_table_query(pool, 'user_data_combined_booking_data;');
        await execute_create_combined_data_query(pool);

        const indexQueries = [
            // SINCE TABLE IS BEING RECREATED DON'T NEED TO DELETE / DROP INDEX
            // drop_idx_user_ptr_id_dates,
            // drop_idx_user_ptr_id_return_date,
            // drop_idx_user_ptr_id_status,
            create_idx_user_ptr_id_dates,
            create_idx_user_ptr_id_return_date,
            create_idx_user_ptr_id_status,
        ];

        console.log(indexQueries.length);

        for (let i = 0; i < indexQueries.length; i++) {
            await execute_create_combined_data_index(pool, indexQueries[i]);
        }
        
        // STEP 3.2: CREATE ROLLUP RUNNING TOTALS GROUP BY DATA
        console.log(`STEP 3.2: CREATE ROLLUP RUNNING TOTALS GROUP BY DATA`);
        console.log(getCurrentDateTime());

        await execute_drop_table_query(pool, 'user_data_key_metrics_rollup;');
        console.log(`Executing user_data_key_metrics_rollup user by id`);
        await execute_create_rollup_data_query(pool);
        await execute_insert_createdAt_query(pool, 'user_data_key_metrics_rollup');   
        
        // STEP 3.3: CREATE USER PROFILE DATA
        console.log(`STEP 3.3: CREATE USER PROFILE DATA`);
        console.log(getCurrentDateTime());

        await execute_drop_table_query(pool, 'user_data_profile;');
        console.log(`Executing create user_data_profile`);
        await execute_create_user_profile_data_query(pool);
        await execute_insert_createdAt_query(pool, 'user_data_profile');   

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
        // MOVED THE MESSAGE BELOW TO THE BOOKING_JOB_032024 PROCESS
        console.log(`\nAll combine user/booking queries executed successfully. Elapsed Time: ${elapsedTime ? elapsedTime : "Opps error getting time"} sec\n`);
        return elapsedTime;
    }
}

// Run the main function
// execute_create_user_data();

module.exports = {
    execute_create_user_data,
}