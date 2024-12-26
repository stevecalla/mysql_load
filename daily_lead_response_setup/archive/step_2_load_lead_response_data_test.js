const fsp = require('fs').promises; // promises necessary for "fs.readdir"
const dotenv = require('dotenv');
dotenv.config({ path: "../../.env" });
const path = require('path');

const { localLeadDbConfig } = require('../utilities/config');
const { createLocalDBConnection } = require('../utilities/connectionLocalDB');
const { getCurrentDateTime } = require('../utilities/getCurrentDate');
const { create_directory } = require('../utilities/createDirectory');

const { query_drop_database, query_drop_table } = require('./queries/create_drop_db_table/queries_drop_db_tables');
const { query_create_database } = require('./queries/create_drop_db_table/queries_create_db');
const { query_load_lead_data } = require('./queries/load_data/query_load_lead_data');

const { tables_library } = require('./queries/create_drop_db_table/query_create_lead_data_table');

const { runTimer, stopTimer } = require('../utilities/timer');

// Connect to MySQL
async function create_connection() {
    console.log('create connection');
    try {
        // Create a connection to MySQL
        const config_details = localLeadDbConfig;
        const pool = createLocalDBConnection(config_details);
        return pool;

    } catch (error) {
        console.log(`Error connecting: ${error}`);
    }
}

// Log memory usage
function logMemoryUsage(label) {
    const memoryUsage = process.memoryUsage();
    console.log(`\n${label} - Memory Usage:`);
    console.log(`RSS: ${(memoryUsage.rss / 1024 / 1024).toFixed(2)} MB`);
    console.log(`Heap Total: ${(memoryUsage.heapTotal / 1024 / 1024).toFixed(2)} MB`);
    console.log(`Heap Used: ${(memoryUsage.heapUsed / 1024 / 1024).toFixed(2)} MB`);
    console.log(`External: ${(memoryUsage.external / 1024 / 1024).toFixed(2)} MB`);
    console.log(`ArrayBuffers: ${(memoryUsage.arrayBuffers).toFixed(2)}\n`);
}

// EXECUTE MYSQL TO CREATE DB QUERY
async function execute_mysql_create_db_query(pool, query, step_info) {
    console.log('create db', query);

    return new Promise((resolve, reject) => {
        const startTime = performance.now();

        pool.query(query, (queryError, results) => {
            const endTime = performance.now();
            const elapsedTime = ((endTime - startTime) / 1_000).toFixed(2); // convert ms to sec

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

// EXECUTE MYSQL TO CREATE TABLES & WORK WITH TABLES QUERY
// async function execute_mysql_working_query(pool, db_name, query, filePath, step_info, rows_added) {
//     const startTime = performance.now();
//     const fs = require('fs');

//     return new Promise((resolve, reject) => {
//         let fileStream;

//         try {
//             if (filePath) {
//                 fileStream = fs.createReadStream(filePath);

//                 fileStream.on('close', () => {
//                     console.log(`File stream for ${filePath} closed successfully.`);
//                 });

//                 fileStream.on('error', (streamError) => {
//                     console.error(`Stream error for file ${filePath}:`, streamError);
//                     reject(streamError);
//                 });
//             }

//             pool.query(`USE ${db_name};`, (queryError) => {
//                 if (queryError) {
//                     console.error('Error switching database:', queryError);
//                     if (fileStream) fileStream.destroy(); // Ensure stream is destroyed in case of error
//                     reject(queryError);
//                     return;
//                 }

//                 const queryConfig = { sql: query };
//                 if (filePath) {
//                     queryConfig.infileStreamFactory = () => fileStream;
//                 }

//                 pool.query(queryConfig, (queryError, results) => {
//                     const endTime = performance.now();
//                     const elapsedTime = ((endTime - startTime) / 1_000).toFixed(2); // Convert ms to sec

//                     if (fileStream) fileStream.destroy(); // Ensure stream is destroyed after query execution

//                     // if (global.gc) {
//                     //     global.gc(); // Trigger garbage collection
//                     //     logMemoryUsage('After garbage collection 1');
//                     // } else {
//                     //     console.warn('Garbage collection is not exposed. Run with --expose-gc to enable.');
//                     // }

//                     if (queryError) {
//                         console.error('Error executing query:', queryError);
//                         reject(queryError);
//                     } else {
//                         console.table(results);
//                         console.log(`Query results: ${results.info}, Elapsed Time: ${elapsedTime} sec\n`);
//                         rows_added = parseInt(results.affectedRows || 0);
//                         resolve(rows_added);
//                     }
//                 });
//             });
//         } catch (err) {
//             if (fileStream) fileStream.destroy(); // Ensure stream is destroyed in case of unexpected errors
//             reject(err);
//         }
//     });
// }

async function execute_mysql_working_query(pool, db_name, query, filePath, step_info, rows_added) {
    const startTime = performance.now();
    const fs = require('fs');

    return new Promise((resolve, reject) => {
        let fileStream;

        try {
            if (filePath) {
                fileStream = fs.createReadStream(filePath);

                fileStream.on('close', () => {
                    console.log(`File stream for ${filePath} closed successfully.`);
                });

                fileStream.on('error', (streamError) => {
                    console.error(`Stream error for file ${filePath}:`, streamError);
                    reject(streamError);
                });
            }

            pool.query(`USE ${db_name};`, (queryError) => {
                if (queryError) {
                    console.error('Error switching database:', queryError);
                    if (fileStream) fileStream.destroy(); // Cleanup on error
                    reject(queryError);
                    return;
                }

                const queryConfig = { sql: query };
                if (filePath) {
                    queryConfig.infileStreamFactory = () => fileStream;
                }

                pool.query(queryConfig, (queryError, results) => {
                    const endTime = performance.now();
                    const elapsedTime = ((endTime - startTime) / 1_000).toFixed(2);

                    if (fileStream) { 
                        fileStream.destroy(); // Cleanup after execution
                        fileStream = null;
                    }

                    if (queryError) {
                        console.error('Error executing query:', queryError);
                        reject(queryError);
                    } else {
                        console.table(results);
                        console.log(`Query results: ${results.info}, Elapsed Time: ${elapsedTime} sec\n`);
                        rows_added = parseInt(results.affectedRows || 0);
                        resolve(rows_added);
                    }
                });
            });
        } catch (err) {
            if (fileStream) fileStream.destroy(); // Cleanup on unexpected error
            reject(err);
        }
    });
}


async function execute_load_lead_response_data() {
    let pool;
    const startTime = performance.now();

    try {
        // STEP #0: CREATE CONNECTION
        pool = await create_connection();
        const db_name = 'ezhire_lead_response_data';
        console.log(db_name);

        // STEP #1: CREATE DATABASE - ONLY NEED TO CREATE DB INITIALLY
        const drop_db = false; // normally don't drop db
        drop_db && await execute_mysql_create_db_query(pool, query_drop_database(db_name), `STEP #1.0: DROP DB`);
        await execute_mysql_create_db_query(pool, query_create_database(db_name), `STEP #1.1: CREATE DATABASE`);

        // STEP #2: CREATE TABLE(S) = all files loaded into single table
        for (const table of tables_library) {
            const { table_name, create_query, step, step_info } = table;

            const drop_query = await query_drop_table(table_name);

            const drop_info = `${step} DROP ${step_info.toUpperCase()} TABLE`;
            await execute_mysql_working_query(pool, db_name, drop_query, null, drop_info);

            const create_info = `${step} CREATE ${step_info.toUpperCase()} TABLE`;
            await execute_mysql_working_query(pool, db_name, create_query, null, create_info);
        }

        // STEP #3 - GET FILES IN DIRECTORY / LOAD INTO "USER DATA" TABLE
        console.log(`STEP #3 - GET FILES IN DIRECTORY / LOAD INTO "ezhire_lead_response_data lead_response_data" TABLE`);
        console.log(getCurrentDateTime());

        logMemoryUsage('Before processing files');

        let rows_added = 0;

        const directory = await create_directory('ezhire_lead_data');
        console.log(directory);

        // List all files in the directory
        const files = await fsp.readdir(directory);
        console.log(files);
        let numer_of_files = 0;

        // Iterate through each file
        for (let i = 0; i < files.length; i++) {

            runTimer(`0_get_data`);

            let currentFile = files[i];

            if (currentFile.endsWith('.csv')) {
                numer_of_files++;

                // Construct the full file path
                let filePath = path.join(directory, currentFile);
                filePath = filePath.replace(/\\/g, '/');

                console.log('file path to insert data = ', filePath);

                let table_name = tables_library[0].table_name;
                const query_load = query_load_lead_data(filePath, table_name);

                // Insert file into "" table
                let query = await execute_mysql_working_query(pool, db_name, query_load, filePath, `STEP #3.${i + 1}: Load data from ${currentFile}`, rows_added);

                // track number of rows added
                rows_added += parseInt(query);
                console.log(`File ${i + 1} of ${files.length}`);
                console.log(`Rows added = ${rows_added}\n`);

                stopTimer(`0_get_data`);
            }
        }

        logMemoryUsage('After processing files');

        console.log('Files processed =', numer_of_files);

        // STEP #5a: Log results
        console.log('STEP #5A: All queries executed successfully.');

    } catch (error) {
        console.log('STEP #5B: All queries NOT executed successfully.');
        console.error('Error:', error);

    } finally {
        // STEP #5c: Log results
        const endTime = performance.now();
        const elapsedTime = ((endTime - startTime) / 1_000).toFixed(2); // convert ms to sec
        console.log(`\nSTEP #5C = TIME LOG. Elapsed Time: ${elapsedTime ? elapsedTime : "Oops error getting time"} sec\n`);

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

        if (global.gc) {
            global.gc(); // Trigger garbage collection
            logMemoryUsage('After garbage collection 2');
        } else {
            console.warn('Garbage collection is not exposed. Run with --expose-gc to enable.');
        }

        return elapsedTime;
    }
}

// node --expose-gc your_script.js
execute_load_lead_response_data();

module.exports = {
    execute_load_lead_response_data,
};