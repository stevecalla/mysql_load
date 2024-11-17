const { generateLogFile } = require('../utilities/generateLogFile');
const { getCurrentDateTime } = require('../utilities/getCurrentDate');
const { slack_message_drissues_channel } = require('../schedule_slack/slack_drissues_channel');
const { slack_message_steve_calla_channel } = require('../schedule_slack/slack_steve_calla_channel');

// const { execute_get_most_recent_created_on_date } = require('../get_booking_data/sql_getBookingMostRecentCreatedOn'); //step_0
const { execute_get_most_recent_created_on_date } = require('../get_most_recent_created_on/sql_getBookingMostRecentCreatedOn'); //step_0
const { execute_get_booking_data } = require('../get_booking_data/sql_getBookingData_ssh_loop'); //step_1
const { execute_load_booking_data } = require('../load_booking_data/sql_load_bookingData'); //step_2
const { execute_create_key_metrics } = require('../create_keyMetrics_data/sql_getKeyMetrics_loop'); //step_3
const { execute_create_pacing_metrics } = require('../create_pacing_data/sql_getPacingMetrics_loop'); //step_4

const { execute_process_user_data } = require('../process_user_data/step_0_process_user_data_042524'); //step_5

const { execute_load_data_to_bigquery } = require('../load_bigquery/move_data_to_bigquery/step_0_load_main_job_040424'); //step 6

let run_step_0 = false;     // get most recent created on / updated on datetime
let run_step_1 = false;     // get booking data
let run_step_2 = false;     // load booking data
let run_step_3 = false;     // create key metrics
let run_step_4 = false;     // create pacing metrics   
let run_step_5 = true;     // process user data = profile, cohort, rfm 
let run_step_6 = false;     // upload data to google cloud / bigquery

async function check_most_recent_created_on_date() {
    const startTime = performance.now();
    console.log(`\n\nPROGRAM START TIME = ${getCurrentDateTime()}`);
    generateLogFile('scheduled_booking_data', `\n\nPROGRAM START TIME = ${getCurrentDateTime()}`);
    
    try {
        // STEP #1: RUN QUERY TO GET MOST RECENT CREATED ON / UPDATED ON DATE
        console.log('\n*************** STARTING STEP 0 ***************\n');

        let log_message_2_hours = "";
        let success_message = "";
        let slackMessage = "";
        
        if (run_step_0) {
            // EXECUTE QUERY
            let getResults = await execute_get_most_recent_created_on_date();
            console.log('query results = ', getResults);
            
            const results = getResults.results[0];
            let { is_within_2_hours } = results;

            log_message_2_hours = await create_log_message(getResults);

            // console.log('message = ', log_message_2_hours);
            // console.log('is within 2 hours = ', is_within_2_hours);

            // if false then 
            if (!is_within_2_hours) { // is within 2 hours is a string not boolean
                // (a) adjust variables to false to prevent running next steps
                // not 100% necessary given return below; used as backup
                run_step_1 = false; // get booking data
                run_step_2 = false; // load booking data
                run_step_3 = false; // create key metrics
                run_step_4 = false; // create pacing metrics 
                run_step_5 = false; // process user data = profile, cohort, rfm
                run_step_6 = false; // upload data to google cloud / bigquery

                // (b) LOGS
                let fail_message = getResults ? `\nHello - Myproject db needs some attention please. DB rental_car_booking2 most recent created on time is outside 2 hours. Elapsed Time: ${getResults.elapsedTime}`: `Opps error getting elapsed time`;

                // (c) send slack with warning
                slackMessage = `${fail_message}\n${log_message_2_hours}`;
                await slack_message_drissues_channel(slackMessage);
                // await slack_message_steve_calla_channel(slackMessage);

                console.log(fail_message);
                console.log(getResults);

                generateLogFile('scheduled_booking_data', fail_message);
                generateLogFile('scheduled_booking_data', log_message_2_hours);

                const endTime = performance.now();
                const elapsedTime = ((endTime - startTime) / 1_000).toFixed(2); //convert ms to sec
                console.log(`\nPROGRAM END TIME: ${getCurrentDateTime()}; ELASPED TIME: ${elapsedTime} sec\n`);
                generateLogFile('scheduled_booking_data', `\nPROGRAM END TIME: ${getCurrentDateTime()}; ELASPED TIME: ${elapsedTime} sec\n`);

                // (c) return to exit function
                return;
            }
    
            // LOGS
            success_message = getResults ? `\nMost recent created on time is within 2 hours. Elapsed Time: ${getResults.elapsedTime}`: `Opps error getting elapsed time\n`;

            console.log(success_message);
            generateLogFile('scheduled_booking_data', success_message);
 
        } else {
            // LOGS
            let skip_message = `\nSkipped STEP 1 due to toggle set to false.\n`;
            console.log(skip_message);
            generateLogFile('scheduled_booking_data', skip_message); 
        }
        
        console.log('\n*************** END OF STEP 1 ***************\n');

        // LOGS & SLACK MESSAGE
        const endTime = performance.now();
        const elapsedTime = ((endTime - startTime) / 1_000).toFixed(2); //convert ms to sec

        let message = `\nSTEP 0 - CHECK MOST RECENT DATE: PROGRAM END TIME: ${getCurrentDateTime()}; ELASPED TIME: ${elapsedTime} sec\n`;

        console.log(`\n${message}\n`);
        generateLogFile('scheduled_booking_data', `${message}\n`);

        slackMessage = `${message}${success_message}\n${log_message_2_hours}`;
        await slack_message_steve_calla_channel(slackMessage);

        // NEXT STEP
        await step_1_get_booking_data(startTime);

    } catch (error) {
        console.error('Error executing Step #1:', error);
        generateLogFile('scheduled_booking_data', `Error executing Step #1: ${error}`);
        return; // Exit the function early
    }

}

async function step_1_get_booking_data(startTime) {
    try {
        // STEP #1: GET BOOKING DATA
        console.log('\n*************** STARTING STEP 1 ***************\n');

        if (run_step_1) {
            // EXECUTE QUERIES
            let getResults;
            getResults = await execute_get_booking_data();
            console.log('get results = ', getResults);
    
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

        // LOGS & SLACK MESSAGE
        const endTime = performance.now();
        const elapsedTime = ((endTime - startTime) / 1_000).toFixed(2); //convert ms to sec

        let message = `\nSTEP 1 - GET BOOKING DATA: PROGRAM END TIME: ${getCurrentDateTime()}; ELASPED TIME: ${elapsedTime} sec\n`;

        console.log(`\n${message}\n`);
        generateLogFile('scheduled_booking_data', `${message}\n`);
        await slack_message_steve_calla_channel(message);

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
        
        // LOGS & SLACK MESSAGE
        const endTime = performance.now();
        const elapsedTime = ((endTime - startTime) / 1_000).toFixed(2); //convert ms to sec

        let message = `\nSTEP 2 - LOAD BOOKING DATA: PROGRAM END TIME: ${getCurrentDateTime()}; ELASPED TIME: ${elapsedTime} sec\n`;

        console.log(`\n${message}\n`);
        generateLogFile('scheduled_booking_data', `${message}\n`);
        await slack_message_steve_calla_channel(message);

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
        
        // LOGS & SLACK MESSAGE
        const endTime = performance.now();
        const elapsedTime = ((endTime - startTime) / 1_000).toFixed(2); //convert ms to sec

        let message = `\nSTEP 3 - CREATE KEY METRICS DATA: PROGRAM END TIME: ${getCurrentDateTime()}; ELASPED TIME: ${elapsedTime} sec\n`;

        console.log(`\n${message}\n`);
        generateLogFile('scheduled_booking_data', `${message}\n`);
        await slack_message_steve_calla_channel(message);

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
        
        // LOGS & SLACK MESSAGE
        const endTime = performance.now();
        const elapsedTime = ((endTime - startTime) / 1_000).toFixed(2); //convert ms to sec

        let message = `\nSTEP 4 - CREATE PACING DATA: PROGRAM END TIME: ${getCurrentDateTime()}; ELASPED TIME: ${elapsedTime} sec\n`;

        console.log(`\n${message}\n`);
        generateLogFile('scheduled_booking_data', `${message}\n`);
        await slack_message_steve_calla_channel(message);

        // NEXT STEP
        await step_5(startTime);

    } catch (error) {
        console.error('Error executing Step #4:', error);
        generateLogFile('scheduled_booking_data', `Error executing Step #4: ${error}`);
        return; // Exit the function early
    }
}

async function step_5(startTime) {
    try {
        // STEP #5: CREATE USER DATA
        console.log('\n*************** STARTING STEP 4 ***************\n');

        if (run_step_5) {
            // EXECUTE QUERIES
            let getResults;
            getResults = await execute_process_user_data();
    
            // LOGS
            let message = getResults ? `\nAll create user data queries executed successfully. Elapsed Time: ${getResults}` : `Opps error getting elapsed time\n`;

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
        
        // LOGS & SLACK MESSAGE
        const endTime = performance.now();
        const elapsedTime = ((endTime - startTime) / 1_000).toFixed(2); //convert ms to sec

        let message = `\nSTEP 5 - CREATE USER DATA: PROGRAM END TIME: ${getCurrentDateTime()}; ELASPED TIME: ${elapsedTime} sec\n`;

        console.log(`\n${message}\n`);
        generateLogFile('scheduled_booking_data', `${message}\n`);
        await slack_message_steve_calla_channel(message);

        // NEXT STEP
        await step_6(startTime);

    } catch (error) {
        console.error('Error executing Step #5:', error);
        generateLogFile('scheduled_booking_data', `Error executing Step #5: ${error}`);
        return; // Exit the function early
    }
}

async function step_6(startTime) {
    try {
        // STEP #6: LOAD DATA TO BIGQUERY
        console.log('\n*************** STARTING STEP 6 ***************\n');

        if (run_step_6) {
            // EXECUTE QUERIES
            let getResults;
            getResults = await execute_load_data_to_bigquery();
    
            // LOGS
            let message = getResults ? `\nData load to BQ executed successfully. Elapsed Time: ${getResults}` : `Opps error getting elapsed time\n`;

            console.log(message);
            console.log('\n*************** END OF STEP 6 ***************\n');
            generateLogFile('scheduled_booking_data', message);
    
        } else {
            // LOGS
            let message = `\nSkipped STEP 6 due to toggle set to false.\n`;

            console.log(message);
            generateLogFile('scheduled_booking_data', message);
        }

        
        console.log('\n*************** END OF STEP 6 ***************\n');
        
        // LOGS & SLACK MESSAGE
        const endTime = performance.now();
        const elapsedTime = ((endTime - startTime) / 1_000).toFixed(2); //convert ms to sec

        let message = `\nSTEP 6 - LOAD DATA TO BIGQUERY: PROGRAM END TIME: ${getCurrentDateTime()}; ELASPED TIME: ${elapsedTime} sec\n`;

        console.log(`\n${message}\n`);
        generateLogFile('scheduled_booking_data', `${message}\n`);
        await slack_message_steve_calla_channel(message);

        // NEXT STEP - No STEP #7
        // await step_7(startTime);

    } catch (error) {
        console.error('Error executing Step #5:', error);
        generateLogFile('scheduled_booking_data', `Error executing Step #4: ${error}`);
        return; // Exit the function early
    }
}

// MESSAGES
async function create_log_message(data) {
    results = data.results[0];

    let { source_field, most_recent_event_update_gst, execution_timestamp_gst, time_stamp_difference_minute, time_stamp_difference_hour, is_within_15_minutes, is_within_2_hours } = results;

    let log_message_2_hours = results ? `\nEXECUTION TIMESTAMP: ${execution_timestamp_gst}\nLAST UPDATED: ${most_recent_event_update_gst}\nTIME STAMP DIFFERENCE - HOURS: ${time_stamp_difference_hour}\nSOURCE FIELD: ${source_field}\nIS WITHIN 2 HOURS: ${is_within_2_hours}` : `Opps no results`;

    return log_message_2_hours;
}

check_most_recent_created_on_date();
