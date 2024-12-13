const fs = require('fs');
const dotenv = require('dotenv');
dotenv.config();

const { localLeadDbConfig } = require('../utilities/config');
const { createLocalDBConnection } = require('../utilities/connectionLocalDB');

const { query_lead_data } = require('./queries/get_slack_lead_data/query_slack_lead_data_121324');
const { group_and_format_data_for_slack } = require('./utility_group_and_format_data_for_slack');
const { create_daily_lead_slack_message, create_daily_lead_response_slack_message } = require('../schedule_slack/slack_daily_lead_message');
const { slack_message_api } = require('../schedule_slack/slack_message_api');

// Connect to MySQL
async function create_connection() {

    console.log('create connection');

    try {
        // Create a connection to MySQL
        const config_details = localLeadDbConfig;

        const pool = createLocalDBConnection(config_details);

        return (pool);

    } catch (error) {
        console.log(`Error connecting: ${error}`)
    }
}

// STEP #1: GET / QUERY DAILY PROMO DATA
async function execute_query_get_lead_data(pool, query) {
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

                console.log(`Query results length: ${results.length}, Elapsed Time: ${elapsedTime} sec`);

                resolve(results);
            }
        });
    });
}

async function execute_get_lead_data(country, date) {
    let pool;
    let results;
    const startTime = performance.now();

    try {
        // STEP #1: GET / QUERY Promo DATA & RETURN RESULTS
        pool = await create_connection();

        // STEP #2: GET DATA FOR SLACK MESSAGE
        const query = await query_lead_data();

        results = await execute_query_get_lead_data(pool, query);
        // console.log(results);

        // STEP #3: CREATE SLACK MESSAGE
        if (results) {
            
            // 2rd parameter is count by first 3 characters or uae, 3nd parameter is date ie 2024-12-05
            const tables = await group_and_format_data_for_slack(results, country, date);

            if (tables.no_data_message) {

                const no_data_message = tables.no_data_message;

                return { no_data_message };
            }

            const slack_message_leads = await create_daily_lead_slack_message(results, tables);

            const slack_message_lead_response = await create_daily_lead_response_slack_message(results, tables);

            // STEP #5: RETURN SLACK MESSAGE TO SLASH ROUTE /get-member-sales TO RESPOND
            return { slack_message_leads, slack_message_lead_response}

        } else {
            const slack_message = "Error - No results";
            await slack_message_api(slack_message, "steve_calla_slack_channel");
        }

    } catch (error) {
        console.error('Error:', error);

        // const slack_message = `Error - No results: error`;
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

        console.log(`\nAll lead data queries executed successfully. Elapsed Time: ${elapsedTime ? elapsedTime : "Oops error getting time"} sec\n`);
    }
}

// Run the main function
// const country = 'bat';
// const date = '';
// execute_get_lead_data(country, date);

module.exports = {
    execute_get_lead_data,
}