const fs = require('fs');
const dotenv = require('dotenv');
dotenv.config();

const { localLeadDbConfig } = require('../utilities/config');
const { createLocalDBConnection } = require('../utilities/connectionLocalDB');

// const { query_lead_metrics_data } = require('./queries/get_slack_lead_data/query_slack_lead_data_121324');
const { query_lead_metrics_data } = require('./queries/get_slack_lead_data/query_slack_lead_data_122424');

const { query_metrics_lead_data } = require('../daily_lead_response_setup/queries/get_slack_lead_data/query_metrics_lead_data_122424');
const { query_drop_table } = require('./queries/create_drop_db_table/queries_drop_db_tables');

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

                console.log(results.length ? `Query results length: ${results.length}` : `Query affected rows: ${results.affectedRows}` + `, Elapsed Time: ${elapsedTime} sec.\n`);

                resolve(results);
            }
        });
    });
}

async function execute_get_lead_data(country = "", date = "") {
    let pool;
    let results;
    const startTime = performance.now();

    try {
        // STEP #1: GET / QUERY Promo DATA & RETURN RESULTS
        pool = await create_connection();

        // STEP #1A: CREATE LEAD METRICS TABLE
        console.log(`\nDROP lead_response_metrics data table.`);
        const drop_query = await query_drop_table('lead_response_metrics_data');
        await execute_mysql_working_query(pool, drop_query);
        
        console.log(`\nCREATE lead_response_metrics data table.`);
        const create_metrics_table_query = await query_metrics_lead_data();
        await execute_mysql_working_query(pool, create_metrics_table_query);

        // STEP #2: GET DATA FOR SLACK MESSAGE
        const query = await query_lead_metrics_data(country, date);
        // console.log(query);

        results = await execute_mysql_working_query(pool, query);
        // console.log(results);
        // console.log('results length = ', results.length)

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
// const country = 'uae';
// const country = null; // should return all countries
// const country = ''; // should return all countries
// const date = '2024-12-12';
// const date = ''; // returns all records
// execute_get_lead_data(country, date);

module.exports = {
    execute_get_lead_data,
}