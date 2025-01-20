const fs = require('fs');
const dotenv = require('dotenv');
dotenv.config({ path: "../.env" });

const { localForecastDbConfig } = require('../utilities/config');
const { createLocalDBConnection } = require('../utilities/connectionLocalDB');

const { query_slack_daily_forecast_data } = require('./queries/get_forecast_metrics_data/query_slack_daily_forecast_data_123024')

const { get_formatted_forcast_data } = require('./format_forecast_data');

const { slack_message_api } = require('../schedule_slack/slack_message_api');

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

async function execute_get_slack_forecast_data() {
    let pool;
    let results;
    const startTime = performance.now();

    try {
        // STEP #1: GET / QUERY Promo DATA & RETURN RESULTS
        pool = await create_connection();

        // STEP #2: GET DATA FOR SLACK MESSAGE
        const query = await query_slack_daily_forecast_data();
        // console.log(query);

        results = await execute_mysql_working_query(pool, query);
        // console.log(results);
        // console.log('results length = ', results.length)

        // STEP #3: CREATE SLACK MESSAGE
        if (results) {
            
            // return / format key stats
            const data = await get_formatted_forcast_data(results);
            console.log('step 3 get slack forecast data', data);

            // STEP #4: RETURN SLACK MESSAGE TO SLASH ROUTE /get-member-sales TO RESPOND
            return { data }

        } else {
            const slack_message = "Error - No results step 3 get slack forecast data #1";
            await slack_message_api(slack_message, "steve_calla_slack_channel");
        }

    } catch (error) {
        console.error('Error:', error);

        // const slack_message = "Error - No results step 3 get slack forecast data #2";
        // await slack_message_api(slack_message, "steve_calla_slack_channel");

        throw error;

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

        console.log(`\nAll slack forecast data queries executed successfully. Elapsed Time: ${elapsedTime ? elapsedTime : "Oops error getting time"} sec\n`);
    }
}

// Run the main function
// execute_get_slack_forecast_data();

module.exports = {
    execute_get_slack_forecast_data,
}