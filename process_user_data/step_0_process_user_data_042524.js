const { generateLogFile } = require('../utilities/generateLogFile');
const { getCurrentDateTime } = require('../utilities/getCurrentDate');
const { slack_message_steve_calla_channel } = require('../schedule_slack/slack_steve_calla_channel');

const { execute_get_user_data } = require('./step_1_get_user_data/step_1_sql_getUserData_ssh_loop'); //step_1
const { execute_load_user_data } = require('./step_2_load_user_data/step_2_sql_load_user_data'); //step_2
const { execute_create_user_data } = require('./step_3_combine_user_booking_data/step_3_sql_combine_user_booking_data_ssh_loop'); //step_3
const { execute_create_cohort_stats } = require('./step_4_create_cohort_data/step_4_sql_create_cohort_data_loop'); //step_4
const { execute_create_rfm_ranking_data } = require('./step_5_create_rfm_ranking_data/step_5_sql_create_rfm_ranking_data_ssh_loop'); //step_5
const { execute_create_rfm_history_data } = require('./step_6_create_rfm_history_data/step_6_sql_create_rfm_history_data_ssh_loop'); //step_6
const { execute_create_rfm_tracking } = require('./step_7_create_rfm_tracking/step_7_sql_create_rfm_tracking_ssh_loop'); //step 7

//TODO:
const run_step_1 = true; // retrieve user_data
const run_step_2 = true; // load user_data
const run_step_3 = true; // create combined user/booking, user key metrics rollup, user profile
const run_step_4 = true; // create cohort base and stats
const run_step_5 = true; // create rfm scores & summary
const run_step_6 = false; // inserted from rfm summary data into history table
const run_step_7 = false; // create rfm tracking

// STEP #1: GET USER DATA
async function execute_process_user_data() {
    const startTime = performance.now();
    console.log(`\n\nPROGRAM START TIME = ${getCurrentDateTime()}`);
    generateLogFile('process_user_data', `\n\nPROGRAM START TIME = ${getCurrentDateTime()}`);

    try {
        // STEP #1: GET USER DATA
        console.log('\n*************** STARTING STEP 1 ***************\n');

        if (run_step_1) {
            // EXECUTE QUERIES
            let getResults;
            getResults = await execute_get_user_data();
    
            // LOGS
            let message = getResults ? `\nUser Data - Step 1: Retrieve user_data successfully. Elapsed Time: ${getResults}`: `Opps error getting elapsed time\n`;

            console.log(message);
            generateLogFile('process_user_data', message);
            await slack_message_steve_calla_channel(message);
 
        } else {
            // LOGS
            let message = `\nSkipped STEP 1 due to toggle set to false.\n`;
            console.log(message);
            generateLogFile('process_user_data', message); 
        }
        
        console.log('\n*************** END OF STEP 1 ***************\n');
        // NEXT STEP
        await step_2(startTime);
    } catch (error) {
        console.error('Error executing Step #1:', error);
        generateLogFile('process_user_data', `Error executing Step #1: ${error}`);
        return; // Exit the function early
    }
}

// STEP #2: LOAD USER DATA TO MYSQL
async function step_2(startTime) {
    try {
        console.log('\n*************** STARTING STEP 2 ***************\n');

        if (run_step_2) {
            // EXECUTE QUERIES
            let getResults;
            getResults = await execute_load_user_data();
    
            // LOGS
            let message = getResults ? `\nUser Data - Step 2: Load user data to mysql executed successfully. Elapsed Time: ${getResults}` : `Opps error getting elapsed time\n`;

            console.log(message);
            generateLogFile('process_user_data', message);
            await slack_message_steve_calla_channel(message);
    
        } else {
            // LOGS
            let message = `\nSkipped STEP 2 due to toggle set to false.\n`;
            console.log(message);
            generateLogFile('process_user_data', message);   
        }
        
        console.log('\n*************** END OF STEP 2 ***************\n');
        // NEXT STEP
        await step_3(startTime);
    } catch (error) {
        console.error('Error executing Step #2:', error);
        generateLogFile('process_user_data', `Error executing Step #2: ${error}`);
        return; // Exit the function early
    }
}

// STEP #3: CREATE KEY METRICS ROLLUP OF USER & BOOKING DATA; CREATE USER PROFILE
async function step_3(startTime) {
    try {
        console.log('\n*************** STARTING STEP 3 ***************\n');

        if (run_step_3) {
            // EXECUTE QUERIES
            let getResults;
            getResults = await execute_create_user_data();
    
            // LOGS
            let message = getResults ? `\nUser Data - Step 3: Create key metrics rollup of user & booking data executed successfully. Elapsed Time: ${getResults}` : `Opps error getting elapsed time\n`;

            console.log(message);
            generateLogFile('process_user_data', message);
            await slack_message_steve_calla_channel(message);
    
        } else {
            // LOGS
            let message = `\nSkipped STEP 3 due to toggle set to false.\n`;
            console.log(message);
            generateLogFile('process_user_data', message);
        }
        
        console.log('\n*************** END OF STEP 3 ***************\n');
        // NEXT STEP
        await step_4(startTime);
    } catch (error) {
        console.error('Error executing Step #3:', error);
        generateLogFile('process_user_data', `Error executing Step #3: ${error}`);
        return; // Exit the function early
    }
}

// STEP #4: CREATE COHORT STATS
async function step_4(startTime) {
    try {
        console.log('\n*************** STARTING STEP 4 ***************\n');

        if (run_step_4) {
            // EXECUTE QUERIES
            let getResults;
            getResults = await execute_create_cohort_stats();
    
            // LOGS
            let message = getResults ? `User Data - Step 4: Cohort stats created successfully. Elapsed Time: ${getResults}` : `Opps error getting elapsed time\n`;

            console.log(message);
            generateLogFile('process_user_data', message);
            await slack_message_steve_calla_channel(message);
            
        } else {
            // LOGS
            let message = `\nSkipped STEP 4 due to toggle set to false.\n`;
            
            console.log(message);
            console.log('\n*************** END OF STEP 4 ***************\n');
            generateLogFile('process_user_data', message);
        }
        
        const endTime = performance.now();
        const elapsedTime = ((endTime - startTime) / 1_000).toFixed(2); //convert ms to sec

        console.log(`\nPROGRAM END TIME: ${getCurrentDateTime()}; ELASPED TIME: ${elapsedTime} sec\n`);

        generateLogFile('process_user_data', `\nPROGRAM END TIME: ${getCurrentDateTime()}; ELASPED TIME: ${elapsedTime} sec\n`);

        console.log('*************** END OF STEP 4 ***************\n');
        
        //NEXT STEP = NONE
        await step_5(startTime);
    } catch (error) {
        console.error('Error executing Step #4:', error);
        generateLogFile('process_user_data', `Error executing Step #4: ${error}`);
        return; // Exit the function early
    }
}

// STEP #5: CREATE RFM RANKING DATA
async function step_5(startTime) {
    try {
        console.log('\n*************** STARTING STEP 5 ***************\n');

        if (run_step_5) {
            // EXECUTE QUERIES
            let getResults;
            getResults = await execute_create_rfm_ranking_data();
    
            // LOGS
            let message = getResults ? `User Data - Step 5: RFM stats created successfully. Elapsed Time: ${getResults}` : `Opps error getting elapsed time\n`;

            console.log(message);
            generateLogFile('process_user_data', message);
            await slack_message_steve_calla_channel(message);
            
        } else {
            // LOGS
            let message = `\nSkipped STEP 5 due to toggle set to false.\n`;
            
            console.log(message);
            console.log('\n*************** END OF STEP 5 ***************\n');
            generateLogFile('process_user_data', message);
        }
        
        const endTime = performance.now();
        const elapsedTime = ((endTime - startTime) / 1_000).toFixed(2); //convert ms to sec

        console.log(`\nPROGRAM END TIME: ${getCurrentDateTime()}; ELASPED TIME: ${elapsedTime} sec\n`);

        generateLogFile('process_user_data', `\nPROGRAM END TIME: ${getCurrentDateTime()}; ELASPED TIME: ${elapsedTime} sec\n`);

        console.log('*************** END OF STEP 5 ***************\n');
        
        //NEXT STEP = NONE
        await step_6(startTime);
    } catch (error) {
        console.error('Error executing Step #5:', error);
        generateLogFile('process_user_data', `Error executing Step #5: ${error}`);
        return; // Exit the function early
    }
}

// STEP #6: CREATE RFM HISTORY DATA
async function step_6(startTime) {
    try {
        console.log('\n*************** STARTING STEP 6 ***************\n');

        if (run_step_6) {
            // EXECUTE QUERIES
            let getResults;
            getResults = await execute_create_rfm_history_data();
    
            // LOGS
            let message = getResults ? `User Data - Step 6: RFM history inserted successfully. Elapsed Time: ${getResults}` : `Opps error getting elapsed time\n`;

            console.log(message);
            generateLogFile('process_user_data', message);
            await slack_message_steve_calla_channel(message);
            
        } else {
            // LOGS
            let message = `\nSkipped STEP 6 due to toggle set to false.\n`;
            
            console.log(message);
            console.log('\n*************** END OF STEP 6 ***************\n');
            generateLogFile('process_user_data', message);
        }
        
        const endTime = performance.now();
        const elapsedTime = ((endTime - startTime) / 1_000).toFixed(2); //convert ms to sec

        console.log(`\nPROGRAM END TIME: ${getCurrentDateTime()}; ELASPED TIME: ${elapsedTime} sec\n`);

        generateLogFile('process_user_data', `\nPROGRAM END TIME: ${getCurrentDateTime()}; ELASPED TIME: ${elapsedTime} sec\n`);

        console.log('*************** END OF STEP 6 ***************\n');
        
        //NEXT STEP = NONE
        await step_6(startTime);
    } catch (error) {
        console.error('Error executing Step #6:', error);
        generateLogFile('process_user_data', `Error executing Step #6: ${error}`);
        return; // Exit the function early
    }
}

// STEP #7: CREATE RFM TRACKING
async function step_7(startTime) {
    try {
        console.log('\n*************** STARTING STEP 7 ***************\n');

        if (run_step_7) {
            // EXECUTE QUERIES
            let getResults;
            getResults = await execute_create_rfm_tracking();
    
            // LOGS
            let message = getResults ? `User Data - Step 7: RFM tracking created successfully. Elapsed Time: ${getResults}` : `Opps error getting elapsed time\n`;

            console.log(message);
            generateLogFile('process_user_data', message);
            await slack_message_steve_calla_channel(message);
            
        } else {
            // LOGS
            let message = `\nSkipped STEP 7 due to toggle set to false.\n`;
            
            console.log(message);
            console.log('\n*************** END OF STEP 7 ***************\n');
            generateLogFile('process_user_data', message);
        }
        
        const endTime = performance.now();
        const elapsedTime = ((endTime - startTime) / 1_000).toFixed(2); //convert ms to sec

        console.log(`\nPROGRAM END TIME: ${getCurrentDateTime()}; ELASPED TIME: ${elapsedTime} sec\n`);

        generateLogFile('process_user_data', `\nPROGRAM END TIME: ${getCurrentDateTime()}; ELASPED TIME: ${elapsedTime} sec\n`);

        console.log('*************** END OF STEP 7 ***************\n');
        
        //NEXT STEP = NONE
    } catch (error) {
        console.error('Error executing Step #7:', error);
        generateLogFile('process_user_data', `Error executing Step #7: ${error}`);
        return; // Exit the function early
    }
}

// execute_process_user_data();

module.exports = {
    execute_process_user_data,
}
