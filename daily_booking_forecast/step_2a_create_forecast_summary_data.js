const fs = require('fs');
const dotenv = require('dotenv');
dotenv.config();

const { localForecastDbConfig } = require('../utilities/config');
const { createLocalDBConnection } = require('../utilities/connectionLocalDB');

const { query_metrics_forecast_data } = require('./queries/get_forecast_metrics_data/query_metrics_forecast_data_123024');

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

// STEP #1: GET / QUERY DAILY PROMO DATA
async function execute_mysql_working_query(pool, query) {
    return new Promise((resolve, reject) => {

        const startTime = performance.now();

        pool.query(query, (queryError, results) => {
            const endTime = performance.now();
            const elapsedTime = ((endTime - startTime) / 1_000).toFixed(2); //convert ms to sec

            if (queryError) {
                console.error('Error executing select query:', queryError);
                reject(queryError);
            } else {

                // console.table(results);
                // console.log(results[0]);

                // console.log(results.length ? `Query results length: ${results.length}` : `Query affected rows: ${results.affectedRows}` + `, Elapsed Time: ${elapsedTime} sec.\n`);

                resolve(results);
            }
        });
    });
}

async function execute_create_forecast_metrics() {
    let pool;
    const startTime = performance.now();

    try {
        runTimer(`get_data`);
        pool = await create_connection();

        // STEP #1: DROP DATA / TABLE
        // query creates table if it doesn't exist then inserts data
        // duplicates are not added based on the alter table unique key
        const query = await query_metrics_forecast_data();
        await execute_mysql_working_query(pool, query);
        
        stopTimer(`get_data`);

    } catch (error) {
        console.error('Error:', error);
        stopTimer(`get_data`);

    } finally {
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

        // LOG RESULTS
        const endTime = performance.now();
        const elapsedTime = ((endTime - startTime) / 1_000).toFixed(2); //convert ms to sec

        console.log(`\nAll lead data queries executed successfully. Elapsed Time: ${elapsedTime ? elapsedTime : "Oops error getting time"} sec\n`);

        return elapsedTime;
    }
}

// Run the main function
// execute_create_forecast_metrics();

module.exports = {
    execute_create_forecast_metrics,
}