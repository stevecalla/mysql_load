const dotenv = require('dotenv');
dotenv.config({ path: "../../.env" }); // adding the path ensures each folder will read the .env file as necessary

const { localUserDbConfig, csvExportPath } = require('../../utilities/config');
const { createLocalDBConnection } = require('../../utilities/connectionLocalDB');
const { getCurrentDateTime } = require('../../utilities/getCurrentDate');
const { generateLogFile } = require('../../utilities/generateLogFile');

const { query_insert_rfm_data } = require('./query_insert_rfm_data');
const { query_create_rfm_table_backup, query_insert_rfm_history_data_backup, query_drop_rfm_table_backup } = require('./query_create_rfm_data_backup');

// STEP #6.1 - INSERT RFM DATA INTO HISTORY TABLE
async function execute_query(pool, query) {
    return new Promise((resolve, reject) => {

        const startTime = performance.now();

        query = query();
        // console.log(query);

        pool.query(query, (queryError, results) => {
            const endTime = performance.now();
            const elapsedTime = ((endTime - startTime) / 1_000).toFixed(2); //convert ms to sec

            if (queryError) {
                console.error('Error executing select query:', queryError);
                reject(queryError);
            } else {
                console.log(`\nInsert RFM history data`);
                console.table(results);
                console.log(`Query results: ${results.info}, Elapsed Time: ${elapsedTime} sec\n`);
                resolve();
            }
        });
    });
}

// Main function to handle SSH connection and execute queries
async function execute_create_rfm_history_data() { 
    let pool;
    const startTime = performance.now();

    try {
        pool = await createLocalDBConnection(localUserDbConfig);
        // console.log(pool.config.connectionConfig.user, pool.config.connectionConfig.database);

        // STEP 6.1: INSERT RFM HISTORY DATA
        console.log(`STEP 6.1: INSERT RFM HISTORY DATA`);
        console.log(getCurrentDateTime());

        console.log(`Insert RFM history data`);
        await execute_query(pool, query_insert_rfm_data); 

        // STEP 6.2: CREATE RFM HISTORY BACKUP
        console.log(`STEP 6.2: CREATE RFM HISTORY BACKUP`);
        console.log(getCurrentDateTime());

        console.log(`Drop RFM history data`);
        await execute_query(pool, query_drop_rfm_table_backup); 

        console.log(`Create rfm history backup table`);
        await execute_query(pool, query_create_rfm_table_backup);

        console.log(`Insert rfm history data into backup table`);
        await execute_query(pool, query_insert_rfm_history_data_backup);
        
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
// execute_create_rfm_history_data();

module.exports = {
    execute_create_rfm_history_data
}