const { generateLogFile } = require('../utilities/generateLogFile');
const { getCurrentDateTime } = require('../utilities/getCurrentDate');
const { slack_message_development_channel } = require('../schedule_slack/slack_development_channel');
const { slack_message_steve_calla_channel } = require('../schedule_slack/slack_steve_calla_channel');

const { execute_get_most_recent_created_on_date } = require('../get_booking_data/sql_getBookingMostRecentCreatedOn'); //step_0
const { execute_get_booking_data } = require('../get_booking_data/sql_getBookingData_ssh_loop'); //step_1
const { execute_load_booking_data } = require('../load_booking_data/sql_load_bookingData'); //step_2
const { execute_create_key_metrics } = require('../create_keyMetrics_data/sql_getKeyMetrics_loop'); //step_3
const { execute_create_pacing_metrics } = require('../create_pacing_data/sql_getPacingMetrics_loop'); //step_4
const { execute_load_data_to_bigquery } = require('../load_bigquery/move_data_to_bigquery/step_0_load_main_job_040424'); //step_5

let run_step_0 = true;     // get most recent created on / updated on datetime
let run_step_1 = true;     // get booking data
let run_step_2 = true;     // load booking data
let run_step_3 = true;     // create key metrics
let run_step_4 = true;     // create pacing metrics   
let run_step_5 = true;      // upload data to google cloud / bigquery

async function check_most_recent_created_on_date() {
    const startTime = performance.now();
    console.log(`\n\nPROGRAM START TIME = ${getCurrentDateTime()}`);
    generateLogFile('scheduled_booking_data', `\n\nPROGRAM START TIME = ${getCurrentDateTime()}`);
    
    try {
        // STEP #1: RUN QUERY TO GET MOST RECENT CREATED ON / UPDATED ON DATE
        console.log('\n*************** STARTING STEP 0 ***************\n');
        
        if (run_step_0) {
            // EXECUTE QUERIES
            let getResults;
            getResults = await execute_get_most_recent_created_on_date();
            
            // let { results } = getResults;

            let { last_updated_utc, execution_timestamp_utc, time_stamp_difference, is_within_2_hours } = getResults.results[0];

            // console.log(is_within_2_hours);

            // if false then 
            if (is_within_2_hours === 'false') {
                // (a) adjust variables to false to prevent running next steps
                // not 100% necessary given return below; used as backup
                run_step_1 = false; // get booking data
                run_step_2 = false; // load booking data
                run_step_3 = false; // create key metrics
                run_step_4 = false; // create pacing metrics 
                run_step_5 = false; // upload data to google cloud / bigquery

                // (b) LOGS
                let fail_message = getResults ? `\nMyproject rental_car_booking2 most recent created on time is outside 2 hours. Elapsed Time: ${getResults.elapsedTime}`: `Opps error getting elapsed time`;
                
                let log_results = getResults ? `LAST UPDATED: ${last_updated_utc}, EXECUTION TIMESTAMP:${execution_timestamp_utc}, TIME STAMP DIFFERENCE: ${time_stamp_difference}, SOURCE FIELD: ${source_field}, IS WITHIN 2 HOURS: ${is_within_2_hours}` : `Opps no results\n`;

                // (c) send slack with warning
                await slack_message_development_channel(fail_message, log_results);

                console.log(fail_message);
                console.log(getResults);

                generateLogFile('scheduled_booking_data', fail_message);
                generateLogFile('scheduled_booking_data', log_results);

                const endTime = performance.now();
                const elapsedTime = ((endTime - startTime) / 1_000).toFixed(2); //convert ms to sec
                console.log(`\nPROGRAM END TIME: ${getCurrentDateTime()}; ELASPED TIME: ${elapsedTime} sec\n`);
                generateLogFile('scheduled_booking_data', `\nPROGRAM END TIME: ${getCurrentDateTime()}; ELASPED TIME: ${elapsedTime} sec\n`);

                // (c) return to exit function
                return;
            }
    
            // LOGS
            let success_message = getResults ? `\nMost recent created on time is within 2 hours. Elapsed Time: ${getResults.elapsedTime}`: `Opps error getting elapsed time\n`;

            console.log(success_message);
            generateLogFile('scheduled_booking_data', success_message);
 
        } else {
            // LOGS
            let skip_message = `\nSkipped STEP 1 due to toggle set to false.\n`;
            console.log(skip_message);
            generateLogFile('scheduled_booking_data', skip_message); 
        }
        
        console.log('\n*************** END OF STEP 1 ***************\n');
        // NEXT STEP
        await step_1_get_booking_data(startTime);
    } catch (error) {
        console.error('Error executing Step #1:', error);
        generateLogFile('scheduled_booking_data', `Error executing Step #1: ${error}`);
        return; // Exit the function early
    }

}

async function step_1_get_booking_data(startTime) {
    // const startTime = performance.now();
    // console.log(`\n\nPROGRAM START TIME = ${getCurrentDateTime()}`);
    // generateLogFile('scheduled_booking_data', `\n\nPROGRAM START TIME = ${getCurrentDateTime()}`);

    try {
        // STEP #1: GET BOOKING DATA
        console.log('\n*************** STARTING STEP 1 ***************\n');

        if (run_step_1) {
            // EXECUTE QUERIES
            let getResults;
            getResults = await execute_get_booking_data();
    
            // LOGS
            let message = getResults ? `\nAll get booking data queries executed successfully. Elapsed Time: ${getResults}`: `Opps error getting elapsed time\n`;

            console.log(message);
            generateLogFile('scheduled_booking_data', message);
 
        } else {
            // LOGS
            let message = `\nSkipped STEP 1 due to toggle set to false.\n`;
            console.log(message);
            generateLogFile('scheduled_booking_data', message); 
        }
        
        console.log('\n*************** END OF STEP 1 ***************\n');
        // NEXT STEP
        await step_2(startTime);
    } catch (error) {
        console.error('Error executing Step #1:', error);
        generateLogFile('scheduled_booking_data', `Error executing Step #1: ${error}`);
        return; // Exit the function early
    }
}

async function step_2(startTime) {

    try {
        // STEP #2: LOAD BOOKING DATA
        console.log('\n*************** STARTING STEP 2 ***************\n');

        if (run_step_2) {
            // EXECUTE QUERIES
            let getResults;
            getResults = await execute_load_booking_data();
    
            // LOGS
            let message = getResults ? `\nAll loading data queries executed successfully. Elapsed Time: ${getResults}` : `Opps error getting elapsed time\n`;

            console.log(message);
            generateLogFile('scheduled_booking_data', message);
    
        } else {
            // LOGS
            let message = `\nSkipped STEP 2 due to toggle set to false.\n`;
            console.log(message);
            generateLogFile('scheduled_booking_data', message);   
        }
        
        console.log('\n*************** END OF STEP 2 ***************\n');
        // NEXT STEP
        await step_3(startTime);
    } catch (error) {
        console.error('Error executing Step #2:', error);
        generateLogFile('scheduled_booking_data', `Error executing Step #2: ${error}`);
        return; // Exit the function early
    }
}

async function step_3(startTime) {

    try {
        // STEP #3: CREATE KEY METRICS / ON RENT DATA
        console.log('\n*************** STARTING STEP 3 ***************\n');

        if (run_step_3) {
            // EXECUTE QUERIES
            let getResults;
            getResults = await execute_create_key_metrics();
    
            // LOGS
            let message = getResults ? `\nAll create key metrics queries executed successfully. Elapsed Time: ${getResults}` : `Opps error getting elapsed time\n`;

            console.log(message);
            generateLogFile('scheduled_booking_data', message);
    
            
        } else {
            // LOGS
            let message = `\nSkipped STEP 3 due to toggle set to false.\n`;
            console.log(message);
            generateLogFile('scheduled_booking_data', message);
        }
        
        console.log('\n*************** END OF STEP 3 ***************\n');
        // NEXT STEP
        await step_4(startTime);
    } catch (error) {
        console.error('Error executing Step #3:', error);
        generateLogFile('scheduled_booking_data', `Error executing Step #3: ${error}`);
        return; // Exit the function early
    }
}

async function step_4(startTime) {

    try {
        // STEP #4: CREATE PACING DATA
        console.log('\n*************** STARTING STEP 4 ***************\n');

        if (run_step_4) {
            // EXECUTE QUERIES
            let getResults;
            getResults = await execute_create_pacing_metrics();
    
            // LOGS
            let message = getResults ? `\nAll create pacing data queries executed successfully. Elapsed Time: ${getResults}` : `Opps error getting elapsed time\n`;

            console.log(message);
            console.log('\n*************** END OF STEP 4 ***************\n');
            generateLogFile('scheduled_booking_data', message);
    
        } else {
            // LOGS
            let message = `\nSkipped STEP 4 due to toggle set to false.\n`;

            console.log(message);
            generateLogFile('scheduled_booking_data', message);
        }

        
        console.log('\n*************** END OF STEP 4 ***************\n');
        // NEXT STEP
        await step_5(startTime);

        // const endTime = performance.now();
        // const elapsedTime = ((endTime - startTime) / 1_000).toFixed(2); //convert ms to sec
        // console.log(`\nPROGRAM END TIME: ${getCurrentDateTime()}; ELASPED TIME: ${elapsedTime} sec\n`);
        // generateLogFile('scheduled_booking_data', `\nPROGRAM END TIME: ${getCurrentDateTime()}; ELASPED TIME: ${elapsedTime} sec\n`);

    } catch (error) {
        console.error('Error executing Step #4:', error);
        generateLogFile('scheduled_booking_data', `Error executing Step #4: ${error}`);
        return; // Exit the function early
    }
}

async function step_5(startTime) {
    try {
        // STEP #5: LOAD DATA TO BIGQUERY
        console.log('\n*************** STARTING STEP 5 ***************\n');

        if (run_step_5) {
            // EXECUTE QUERIES
            let getResults;
            getResults = await execute_load_data_to_bigquery();
    
            // LOGS
            let message = getResults ? `\nData load to BQ executed successfully. Elapsed Time: ${getResults}` : `Opps error getting elapsed time\n`;

            console.log(message);
            console.log('\n*************** END OF STEP 5 ***************\n');
            generateLogFile('scheduled_booking_data', message);
    
        } else {
            // LOGS
            let message = `\nSkipped STEP 5 due to toggle set to false.\n`;

            console.log(message);
            generateLogFile('scheduled_booking_data', message);
        }

        
        console.log('\n*************** END OF STEP 5 ***************\n');
        // NEXT STEP - No STEP #6
        // await step_6(startTime);

        const endTime = performance.now();
        const elapsedTime = ((endTime - startTime) / 1_000).toFixed(2); //convert ms to sec

        let message = `\nBOOKING JOB: PROGRAM END TIME: ${getCurrentDateTime()}; ELASPED TIME: ${elapsedTime} sec\n`;

        console.log(`\n${message}\n`);
        generateLogFile('scheduled_booking_data', `${message}\n`);
        await slack_message_steve_calla_channel(message);

    } catch (error) {
        console.error('Error executing Step #5:', error);
        generateLogFile('scheduled_booking_data', `Error executing Step #4: ${error}`);
        return; // Exit the function early
    }
}

check_most_recent_created_on_date();
// get_booking_data();
