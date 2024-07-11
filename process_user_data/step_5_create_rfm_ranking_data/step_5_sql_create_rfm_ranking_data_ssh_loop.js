const dotenv = require('dotenv');
dotenv.config({ path: "../../.env" }); // adding the path ensures each folder will read the .env file as necessary

const { localUserDbConfig, csvExportPath } = require('../../utilities/config');
const { createLocalDBConnection } = require('../../utilities/connectionLocalDB');
const { getCurrentDateTime } = require('../../utilities/getCurrentDate');
const { generateLogFile } = require('../../utilities/generateLogFile');

const { query_create_rfm_ranking } = require('./query_create_rfm_ranking');
const { query_create_rfm_score_summary_data } = require('./query_create_rfm_score_summary_data');

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
                console.log(`\nDrop table results = ${table}`);
                console.table(results);
                console.log(`Query results: ${results.info}, Elapsed Time: ${elapsedTime} sec\n`);
                resolve();
            }
        })
    });
}

// STEP #5.1 - CREATE RFM RANKING
async function execute_create_rfm_ranking(pool, table, metric, metric_as) {
    return new Promise((resolve, reject) => {

        const startTime = performance.now();
  
        const query = query_create_rfm_ranking(table, metric, metric_as);
        // console.log(query);

        pool.query(query, (queryError, results) => {
            const endTime = performance.now();
            const elapsedTime = ((endTime - startTime) / 1_000).toFixed(2); //convert ms to sec

            if (queryError) {
                console.error('Error executing select query:', queryError);
                reject(queryError);
            } else {
                
                console.log(`\nCreate ${table}`);
                console.table(results);
                console.log(`Query results: ${results.info}, Elapsed Time: ${elapsedTime} sec\n`);
                resolve();
            }
        });
    });
}

// STEP #5.2 - CREATE RFM SCORE SUMMARY DATA
async function execute_create_rfm_score_summary_data(pool) {
    return new Promise((resolve, reject) => {

        const startTime = performance.now();

        const query = query_create_rfm_score_summary_data();

        pool.query(query, (queryError, results) => {
            const endTime = performance.now();
            const elapsedTime = ((endTime - startTime) / 1_000).toFixed(2); //convert ms to sec

            if (queryError) {
                console.error('Error executing select query:', queryError);
                reject(queryError);
            } else {
                console.log('\nCreate RFM score summary data');
                console.table(results);
                console.log(`Query results: ${results.info}, Elapsed Time: ${elapsedTime} sec\n`);
                resolve();
            }
        });
    });
}

// Main function to handle SSH connection and execute queries
async function execute_create_rfm_ranking_data() { 
    let pool;
    const startTime = performance.now();

    try {
        pool = await createLocalDBConnection(localUserDbConfig);
        // console.log(pool.config.connectionConfig.user, pool.config.connectionConfig.database);

        // STEP 5.1: CREATE RANKING DATA
        console.log(`STEP 6.1: CREATE RFM RANKING DATA`);
        console.log(getCurrentDateTime());

        const rfm_table_library = [
            {
                table: "rfm_score_recency_data",
                metric: "recency",
                metric_as: "booking_most_recent_return_vs_now"
            },
            {
                table: "rfm_score_frequency_data",
                metric: "frequency",
                metric_as: "total_days_per_completed_and_started_bookings"
            },
            {
                table: "rfm_score_monetary_data",
                metric: "monetary",
                metric_as: "booking_charge__less_discount_aed_per_completed_started_bookings"
            }
        ];
        
        for(let i = 0; i < rfm_table_library.length; i++) {
            let {table, metric, metric_as} = rfm_table_library[i];
            await execute_drop_table_query(pool, table);
            await execute_create_rfm_ranking(pool, table, metric, metric_as);
            await execute_insert_createdAt_query(pool, table); 
        }
        
        // STEP #5.2 - CREATE RFM SCORE SUMMARY DATA
        console.log(`STEP #6.2 - CREATE RFM SCORE SUMMARY DATA`);
        console.log(getCurrentDateTime());

        await execute_drop_table_query(pool, 'rfm_score_summary_data;');

        console.log(`Creating rfm_score_summary_data`);
        await execute_create_rfm_score_summary_data(pool);

        // await execute_insert_createdAt_query(pool, 'rfm_score_summary_data');   

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
        
        console.log(`\nAll rfm ranking and combined rfm data executed successfully. Elapsed Time: ${elapsedTime ? elapsedTime : "Opps error getting time"} sec\n`);
        return elapsedTime;
    }
}

// Run the main function
// execute_create_rfm_ranking_data();

module.exports = {
    execute_create_rfm_ranking_data
}