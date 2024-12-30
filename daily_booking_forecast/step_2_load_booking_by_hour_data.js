const fsp = require('fs').promises; // promses necessary for "fs.readdir"
const dotenv = require('dotenv');
dotenv.config({ path: "../../.env" });
const path = require('path');

const { localForecastDbConfig } = require('../utilities/config');
const { createLocalDBConnection } = require('../utilities/connectionLocalDB');
const { getCurrentDateTime } = require('../utilities/getCurrentDate');
const { create_directory } = require('../utilities/createDirectory');

const { query_drop_database, query_drop_table } = require('./queries/create_drop_db_table/queries_drop_db_tables');
const { query_create_database } = require('./queries/create_drop_db_table/queries_create_db');
const { query_load_booking_by_hour_data } = require('./queries/load_data/query_load_lead_data');

const { tables_library } = require('./queries/create_drop_db_table/query_create_booking_by_hour_table');

const { runTimer, stopTimer } = require('../utilities/timer');

// Connect to MySQL
async function create_connection() {

    console.log('create connection');

    try {
        // Create a connection to MySQL
        const config_details = localForecastDbConfig;

        const pool = createLocalDBConnection(config_details);

        return (pool);

    } catch (error) {
        console.log(`Error connecting: ${error}`)
    }
}

// EXECUTE MYSQL TO CREATE DB QUERY
async function execute_mysql_create_db_query(pool, query, step_info) {

    console.log('create db', query);
    
    return new Promise((resolve, reject) => {

        const startTime = performance.now();

        pool.query(query, (queryError, results) => {
            const endTime = performance.now();
            const elapsedTime = ((endTime - startTime) / 1_000).toFixed(2); //convert ms to sec

            if (queryError) {
                console.error('Error executing create query:', queryError);
                reject(queryError);
            } else {
                console.log(`\n${step_info}`);
                console.table(results);
                console.log(`Query results: ${results.info}, Elapsed Time: ${elapsedTime} sec\n`);
                resolve();
            }
        });
    });
}

// EXECUTE MYSQL TO CREATE TABLES & WORK WITH TABLES QUERY
async function execute_mysql_working_query(pool, db_name, query, filePath, step_info, rows_added) {
    const startTime = performance.now();
    const fs = require('fs');

    return new Promise((resolve, reject) => {
        pool.query(`USE ${db_name};`, (queryError, results) => {
            // pool.query(query, (queryError, results) => {
            pool.query({
                sql: query,
                infileStreamFactory: function() { return fs.createReadStream(filePath)}}, 
                (queryError, results) => {

                const endTime = performance.now();
                const elapsedTime = ((endTime - startTime) / 1_000).toFixed(2); //convert ms to sec

                if (queryError) {
                    console.error('Error executing select query:', queryError);

                    reject(queryError);
                } else {
                    // console.log(`\n${step_info}`);
                    // console.table(results);
                    // console.log(`Query results: ${results.info}, Elapsed Time: ${elapsedTime} sec\n`);

                    // stopTimer(i);
                    rows_added = parseInt(results.affectedRows);
                    resolve(rows_added);
                }
            });
        });
    });
}

async function execute_load_booking_by_hour_data() {
    let pool;
    const startTime = performance.now();

    try {
        // STEP #0: CREATE CONNECTION
        pool = await create_connection();

        const db_name = 'ezhire_forecast_data';
        console.log(db_name);

        // STEP #1: CREATE DATABASE - ONLY NEED TO CREATE DB INITIALLY
        const drop_db = false; // normally don't drop db
        drop_db && await execute_mysql_create_db_query(pool, query_drop_database(db_name), `STEP #1.0: DROP DB`);
        await execute_mysql_create_db_query(pool, query_create_database(db_name), `STEP #1.1: CREATE DATABASE`);
        
        // // STEP #2: CREATE TABLE(S) = all files loaded into single table
        for (const table of tables_library) {
            const { table_name, create_query, step, step_info } = table;

            const drop_query = await query_drop_table(table_name);

            const drop_info = `${step} DROP ${step_info.toUpperCase()} TABLE`;
            await execute_mysql_working_query(pool, db_name, drop_query, drop_info);

            const create_info = `${step} CREATE ${step_info.toUpperCase()} TABLE`;
            await execute_mysql_working_query(pool, db_name, create_query, create_info);
        }

        // // STEP #3 - GET FILES IN DIRECTORY / LOAD INTO "USER DATA" TABLE
        console.log(`STEP #3 - GET FILES IN DIRECTORY / LOAD INTO "ezhire_forecast_data booking by hour data" TABLE`);
        console.log(getCurrentDateTime());

        let rows_added = 0;

        const directory = await create_directory('ezhire_booking_hourly_data');
        console.log(directory);

        // List all files in the directory
        const files = await fsp.readdir(directory);
        console.log(files);
        let numer_of_files = 0;

        // Iterate through each file
        for (let i = 0; i < files.length; i++) {

            runTimer(`get_data`);

            let currentFile = files[i];

            if (currentFile.endsWith('.csv')) {
                numer_of_files++;

                // Construct the full file path
                let filePath = path.join(directory, currentFile);
                filePath = filePath.replace(/\\/g, '/');

                console.log('file path to insert data = ', filePath);

                let table_name = tables_library[0].table_name;
                
                const query_load = query_load_booking_by_hour_data(filePath, table_name);

                // Insert file into "" table
                let query = await execute_mysql_working_query(pool, db_name, query_load, filePath, rows_added, i);

                // track number of rows added
                rows_added += parseInt(query);
                console.log(`File 0 of ${files.length}`);
                console.log(`Rows added = ${rows_added}\n`);

                stopTimer(`get_data`);
            }
        }

        // // generateLogFile('loading_ezhire_lead_data', `Total files added = ${numer_of_files} Total rows added = ${rows_added.toLocaleString()}`, csv_export_path);
        console.log('Files processed =', numer_of_files);

        // STEP #5a: Log results
        console.log('STEP #5A: All queries executed successfully.');

    } catch (error) {
        console.log('STEP #5B: All queries NOT executed successfully.');
        console.error('Error:', error);
        stopTimer(`get_data`);

    } finally {
        // STEP #5c: Log results
        const endTime = performance.now();
        const elapsedTime = ((endTime - startTime) / 1_000).toFixed(2); //convert ms to sec
        console.log(`\nSTEP #5C = TIME LOG. Elapsed Time: ${elapsedTime ? elapsedTime : "Opps error getting time"} sec\n`);

        // STEP #6: CLOSE CONNECTION/POOL
        // Ensure cleanup happens even if there is an error
        if (pool) {
            await new Promise((resolve, reject) => {
                pool.end(err => {
                    if (err) {
                        console.error('Error closing connection pool:', err.message);
                        reject(err);
                    } else {
                        console.log('Connection pool closed successfully.');
                        resolve();
                    }
                });
            });
        }

        return elapsedTime;
    }
}

// execute_load_booking_by_hour_data();

module.exports = {
    execute_load_booking_by_hour_data,
}
