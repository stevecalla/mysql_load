const dotenv = require('dotenv');
dotenv.config({ path: "../../.env" }); // adding the path ensures each folder will read the .env file as necessary

const { localUserDbConfig, csvExportPath } = require('../../utilities/config');
const { createLocalDBConnection } = require('../../utilities/connectionLocalDB');
const { getCurrentDateTime, getFormattedDate } = require('../../utilities/getCurrentDate');
const { generateLogFile } = require('../../utilities/generateLogFile');

const {
    query_drop_rfm_score_summary_history_data_tracking_backup,
    query_create_rfm_score_summary_history_data_tracking_backup,
    query_insert_rfm_score_summary_history_data_tracking_backup,
} = require('./query_create_rfm_tracking_backup');
const {
	query_drop_rfm_score_summary_history_data_tracking,
	query_get_min_and_max_created_at_dates,
	query_get_most_recent_min_and_max_created_at_dates,
    query_get_offer_min_and_max_created_at_dates,
	query_create_rfm_score_summary_history_data_tracking,
} = require('./query_create_rfm_tracking');

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

// STEP #7.1 - CREATE RFM TRACKING
async function execute_query(pool, query, table = "", min_created_at_date = "", min_created_at_date_formatted = "", max_created_at_date = "") {
    return new Promise((resolve, reject) => {

        const startTime = performance.now();

        // console.log('dates = ', min_created_at_date, min_created_at_date_formatted, max_created_at_date);
        
        query = query(table, min_created_at_date, min_created_at_date_formatted, max_created_at_date);
        // console.log(query);


        pool.query(query, (queryError, results) => {
            const endTime = performance.now();
            const elapsedTime = ((endTime - startTime) / 1_000).toFixed(2); //convert ms to sec

            if (queryError) {
                console.error('Error executing select query:', queryError);
                reject(queryError);
            } else {
                console.log(`\nDrop RFM history data`);
                console.table(results);
                console.log(`Query results: ${results.info}, Elapsed Time: ${elapsedTime} sec\n`);
                resolve(results);
            }
        });
    });
}

// Main function to handle SSH connection and execute queries
async function execute_create_rfm_tracking() {
    let pool;
    const startTime = performance.now();

    try {
        pool = await createLocalDBConnection(localUserDbConfig);
        // console.log(pool.config.connectionConfig.user, pool.config.connectionConfig.database);
        
        const tableList = [
            `rfm_score_summary_history_data_tracking`, 
            `rfm_score_summary_history_data_tracking_most_recent`,
            `rfm_score_summary_history_data_tracking_offer`, 
        ];

        // STEP 7.1: CREATE RFM TRACKING SEGMENTS BACKUP
        console.log(`STEP 7.1: CREATE RFM TRACKING SEGMENTS BACKUP`);
        console.log(getCurrentDateTime());

        for (let i = 0; i < tableList.length; i++) {
            table = tableList[i];

            console.log(`DROP RFM rfm_score_summary_history_data_tracking_backup`);
            await execute_query(pool, query_drop_rfm_score_summary_history_data_tracking_backup, table);

            console.log(`CREATE rfm_score_summary_history_data_tracking_backup`);
            await execute_query(pool, query_create_rfm_score_summary_history_data_tracking_backup, table);

            console.log(`INSERT rfm_score_summary_history_data_tracking_backup`);
            await execute_query(pool, query_insert_rfm_score_summary_history_data_tracking_backup, table);
        }

        // STEP 7.2: CREATE RFM TRACKING SEGMENTS
        console.log(`7.2: CREATE RFM TRACKING SEGMENTS`);
        console.log(getCurrentDateTime());

        for (let i = 0; i < tableList.length; i++) {
            table = tableList[i];

            // DROP OLD TABLE
            console.log(`DROP ${table}`);
            await execute_query(pool, query_drop_rfm_score_summary_history_data_tracking, table);
    
            // GET MIN & MAX DATES TO CREATE THE COMPARISON
            console.log(`Get min & max dates for rfm segments`);
            let dates = "";
            if (table === `rfm_score_summary_history_data_tracking`) {
                dates = await execute_query(pool, query_get_min_and_max_created_at_dates, table);
            } else if (table === `rfm_score_summary_history_data_tracking_offer`) {
                dates = await execute_query(pool, query_get_offer_min_and_max_created_at_dates, table);
            } else {
                dates = await execute_query(pool, query_get_most_recent_min_and_max_created_at_dates, table);
            }

            const { min_created_at_date, max_created_at_date, max_created_at_date_plus_1 } = dates[0];
    
            // CREATE RFM TRACKING SEGMENTS
            console.log(`Create rfm_score_summary_history_data_tracking`);
            await execute_query(pool, query_create_rfm_score_summary_history_data_tracking, table, min_created_at_date, max_created_at_date, max_created_at_date_plus_1);

            await execute_insert_createdAt_query(pool, table);
        }

        // *******************

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
// execute_create_rfm_tracking();

module.exports = {
    execute_create_rfm_tracking
}