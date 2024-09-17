const { generateLogFile } = require('../utilities/generateLogFile');
const { getCurrentDateTime } = require('../utilities/getCurrentDate');

const { slack_message_drissues_channel } = require('../schedule_slack/slack_drissues_channel');
const { slack_message_steve_calla_channel } = require('../schedule_slack/slack_steve_calla_channel');
const { slack_message_325_bookings_channel } = require('../schedule_slack/slack_325_bookings_channel');

const { execute_get_most_recent_created_on_date } = require('../get_booking_data/sql_getBookingMostRecentCreatedOn'); //step_0
const { execute_get_daily_booking_data } = require('../daily_booking_forecast/step_1_sql_get_daily_booking_data'); //step_1

let run_step_0 = true;     // get most recent created on / updated on datetime
let run_step_1 = true;     // get daily booking data
// let run_step_2 = true;     // tbd
// let run_step_3 = true;     // tbd
// let run_step_4 = true;     // tbd  
// let run_step_5 = true;     // tbd
// let run_step_6 = true;     // tbd

async function check_most_recent_created_on_date() {
    const startTime = performance.now();
    console.log(`\n\nPROGRAM START TIME = ${getCurrentDateTime()}`);
    generateLogFile('scheduled_booking_data', `\n\nPROGRAM START TIME = ${getCurrentDateTime()}`);
    
    try {
        // STEP #1: RUN QUERY TO GET MOST RECENT CREATED ON / UPDATED ON DATE
        console.log('\n*************** STARTING STEP 0 ***************\n');

        let log_results = "";
        let success_message = "";
        let slackMessage = "";
        
        if (run_step_0) {
            // EXECUTE QUERIES
            let getResults;
            getResults = await execute_get_most_recent_created_on_date();
            
            // let { results } = getResults;

            let { last_updated_utc, execution_timestamp_utc, time_stamp_difference, source_field, is_within_2_hours } = getResults.results[0];
            
            log_results = getResults ? `\nLAST UPDATED: ${last_updated_utc}\nEXECUTION TIMESTAMP: ${execution_timestamp_utc}\nTIME STAMP DIFFERENCE: ${time_stamp_difference}\nSOURCE FIELD: ${source_field}\nIS WITHIN 2 HOURS: ${is_within_2_hours}` : `Opps no results`;

            // console.log(is_within_2_hours);

            // if false then 
            if (is_within_2_hours === 'false') {
                // (a) adjust variables to false to prevent running next steps
                // not 100% necessary given return below; used as backup
                run_step_1 = false; // get booking data
                // run_step_2 = false; // tbd
                // run_step_3 = false; // tbd
                // run_step_4 = false; // tbd
                // run_step_5 = false; // tbd
                // run_step_6 = false; // tbd

                // (b) LOGS
                let fail_message = getResults ? `\nHello - Myproject db needs some attention please. DB rental_car_booking2 most recent created on time is outside 2 hours. Elapsed Time: ${getResults.elapsedTime}`: `Opps error getting elapsed time`;

                // (c) send slack with warning
                slackMessage = `${fail_message}\n${log_results}`;
                await slack_message_drissues_channel(slackMessage);
                await slack_message_steve_calla_channel(slackMessage);

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

        slackMessage = `${message}${success_message}\n${log_results}`;
        await slack_message_steve_calla_channel(slackMessage);

        // NEXT STEP
        await step_1_get_daily_booking_data(startTime);

    } catch (error) {
        console.error('Error executing Step #1:', error);
        generateLogFile('scheduled_booking_data', `Error executing Step #1: ${error}`);
        return; // Exit the function early
    }

}

async function step_1_get_daily_booking_data(startTime) {
    try {
        // STEP #1: GET DAILY BOOKING DATA
        console.log('\n*************** STARTING STEP 1 ***************\n');

        let getResults;

        if (run_step_1) {
            // EXECUTE QUERIES
            getResults = await execute_get_daily_booking_data();
            console.table(getResults);
    
            // LOGS
            let message = getResults ? `\nAll get booking data queries executed successfully.`: `Opps error getting elapsed time\n`;

            console.log(message);
            generateLogFile('scheduled_daily_booking_data', message);
 
        } else {
            // LOGS
            let message = `\nSkipped STEP 1 due to toggle set to false.\n`;
            console.log(message);
            generateLogFile('scheduled_daily_booking_data', message); 
        }
        
        console.log('\n*************** END OF STEP 1 ***************\n');

        // LOGS & SLACK MESSAGE
        const endTime = performance.now();
        const elapsedTime = ((endTime - startTime) / 1_000).toFixed(2); //convert ms to sec

        let message = `\nSTEP 1 - GET DAILY BOOKING DATA: PROGRAM END TIME: ${getCurrentDateTime()}; ELASPED TIME: ${elapsedTime} sec\n`;

        console.log(`\n${message}\n`);
        generateLogFile('scheduled_daily_booking_data', `${message}\n`);

        let slackMessage = await createSlackMessage(getResults)
        await slack_message_steve_calla_channel(slackMessage);
        // await slack_message_325_bookings_channel(slackMessage);

        // NEXT STEP
        // await step_2(startTime);
        

    } catch (error) {
        console.error('Error executing Step #1:', error);
        generateLogFile('scheduled_daily_booking_data', `Error executing Step #1: ${error}`);
        return; // Exit the function early
    }
}

// async function step_2(startTime) {
//     try {
//         // STEP #2: LOAD BOOKING DATA
//         console.log('\n*************** STARTING STEP 2 ***************\n');

//         if (run_step_2) {
//             // EXECUTE QUERIES
//             let getResults;
//             getResults = await execute_load_booking_data();
    
//             // LOGS
//             let message = getResults ? `\nAll loading data queries executed successfully. Elapsed Time: ${getResults}` : `Opps error getting elapsed time\n`;

//             console.log(message);
//             generateLogFile('scheduled_booking_data', message);
    
//         } else {
//             // LOGS
//             let message = `\nSkipped STEP 2 due to toggle set to false.\n`;
//             console.log(message);
//             generateLogFile('scheduled_booking_data', message);   
//         }
        
//         console.log('\n*************** END OF STEP 2 ***************\n');
        
//         // LOGS & SLACK MESSAGE
//         const endTime = performance.now();
//         const elapsedTime = ((endTime - startTime) / 1_000).toFixed(2); //convert ms to sec

//         let message = `\nSTEP 2 - LOAD BOOKING DATA: PROGRAM END TIME: ${getCurrentDateTime()}; ELASPED TIME: ${elapsedTime} sec\n`;

//         console.log(`\n${message}\n`);
//         generateLogFile('scheduled_booking_data', `${message}\n`);
//         await slack_message_steve_calla_channel(message);

//         // NEXT STEP
//         await step_3(startTime);

//     } catch (error) {
//         console.error('Error executing Step #2:', error);
//         generateLogFile('scheduled_booking_data', `Error executing Step #2: ${error}`);
//         return; // Exit the function early
//     }
// }

// async function step_3(startTime) {
//     try {
//         // STEP #3: CREATE KEY METRICS / ON RENT DATA
//         console.log('\n*************** STARTING STEP 3 ***************\n');

//         if (run_step_3) {
//             // EXECUTE QUERIES
//             let getResults;
//             getResults = await execute_create_key_metrics();
    
//             // LOGS
//             let message = getResults ? `\nAll create key metrics queries executed successfully. Elapsed Time: ${getResults}` : `Opps error getting elapsed time\n`;

//             console.log(message);
//             generateLogFile('scheduled_booking_data', message);
    
            
//         } else {
//             // LOGS
//             let message = `\nSkipped STEP 3 due to toggle set to false.\n`;
//             console.log(message);
//             generateLogFile('scheduled_booking_data', message);
//         }
        
//         console.log('\n*************** END OF STEP 3 ***************\n');
        
//         // LOGS & SLACK MESSAGE
//         const endTime = performance.now();
//         const elapsedTime = ((endTime - startTime) / 1_000).toFixed(2); //convert ms to sec

//         let message = `\nSTEP 3 - CREATE KEY METRICS DATA: PROGRAM END TIME: ${getCurrentDateTime()}; ELASPED TIME: ${elapsedTime} sec\n`;

//         console.log(`\n${message}\n`);
//         generateLogFile('scheduled_booking_data', `${message}\n`);
//         await slack_message_steve_calla_channel(message);

//         // NEXT STEP
//         await step_4(startTime);

//     } catch (error) {
//         console.error('Error executing Step #3:', error);
//         generateLogFile('scheduled_booking_data', `Error executing Step #3: ${error}`);
//         return; // Exit the function early
//     }
// }

// async function step_4(startTime) {
//     try {
//         // STEP #4: CREATE PACING DATA
//         console.log('\n*************** STARTING STEP 4 ***************\n');

//         if (run_step_4) {
//             // EXECUTE QUERIES
//             let getResults;
//             getResults = await execute_create_pacing_metrics();
    
//             // LOGS
//             let message = getResults ? `\nAll create pacing data queries executed successfully. Elapsed Time: ${getResults}` : `Opps error getting elapsed time\n`;

//             console.log(message);
//             console.log('\n*************** END OF STEP 4 ***************\n');
//             generateLogFile('scheduled_booking_data', message);
    
//         } else {
//             // LOGS
//             let message = `\nSkipped STEP 4 due to toggle set to false.\n`;

//             console.log(message);
//             generateLogFile('scheduled_booking_data', message);
//         }

//         console.log('\n*************** END OF STEP 4 ***************\n');
        
//         // LOGS & SLACK MESSAGE
//         const endTime = performance.now();
//         const elapsedTime = ((endTime - startTime) / 1_000).toFixed(2); //convert ms to sec

//         let message = `\nSTEP 4 - CREATE PACING DATA: PROGRAM END TIME: ${getCurrentDateTime()}; ELASPED TIME: ${elapsedTime} sec\n`;

//         console.log(`\n${message}\n`);
//         generateLogFile('scheduled_booking_data', `${message}\n`);
//         await slack_message_steve_calla_channel(message);

//         // NEXT STEP
//         await step_5(startTime);

//     } catch (error) {
//         console.error('Error executing Step #4:', error);
//         generateLogFile('scheduled_booking_data', `Error executing Step #4: ${error}`);
//         return; // Exit the function early
//     }
// }

// async function step_5(startTime) {
//     try {
//         // STEP #5: CREATE USER DATA
//         console.log('\n*************** STARTING STEP 4 ***************\n');

//         if (run_step_5) {
//             // EXECUTE QUERIES
//             let getResults;
//             getResults = await execute_process_user_data();
    
//             // LOGS
//             let message = getResults ? `\nAll create user data queries executed successfully. Elapsed Time: ${getResults}` : `Opps error getting elapsed time\n`;

//             console.log(message);
//             console.log('\n*************** END OF STEP 5 ***************\n');
//             generateLogFile('scheduled_booking_data', message);
    
//         } else {
//             // LOGS
//             let message = `\nSkipped STEP 5 due to toggle set to false.\n`;

//             console.log(message);
//             generateLogFile('scheduled_booking_data', message);
//         }
        
//         console.log('\n*************** END OF STEP 5 ***************\n');
        
//         // LOGS & SLACK MESSAGE
//         const endTime = performance.now();
//         const elapsedTime = ((endTime - startTime) / 1_000).toFixed(2); //convert ms to sec

//         let message = `\nSTEP 5 - CREATE USER DATA: PROGRAM END TIME: ${getCurrentDateTime()}; ELASPED TIME: ${elapsedTime} sec\n`;

//         console.log(`\n${message}\n`);
//         generateLogFile('scheduled_booking_data', `${message}\n`);
//         await slack_message_steve_calla_channel(message);

//         // NEXT STEP
//         await step_6(startTime);

//     } catch (error) {
//         console.error('Error executing Step #5:', error);
//         generateLogFile('scheduled_booking_data', `Error executing Step #5: ${error}`);
//         return; // Exit the function early
//     }
// }

// async function step_6(startTime) {
//     try {
//         // STEP #6: LOAD DATA TO BIGQUERY
//         console.log('\n*************** STARTING STEP 6 ***************\n');

//         if (run_step_6) {
//             // EXECUTE QUERIES
//             let getResults;
//             getResults = await execute_load_data_to_bigquery();
    
//             // LOGS
//             let message = getResults ? `\nData load to BQ executed successfully. Elapsed Time: ${getResults}` : `Opps error getting elapsed time\n`;

//             console.log(message);
//             console.log('\n*************** END OF STEP 6 ***************\n');
//             generateLogFile('scheduled_booking_data', message);
    
//         } else {
//             // LOGS
//             let message = `\nSkipped STEP 6 due to toggle set to false.\n`;

//             console.log(message);
//             generateLogFile('scheduled_booking_data', message);
//         }

        
//         console.log('\n*************** END OF STEP 6 ***************\n');
        
//         // LOGS & SLACK MESSAGE
//         const endTime = performance.now();
//         const elapsedTime = ((endTime - startTime) / 1_000).toFixed(2); //convert ms to sec

//         let message = `\nSTEP 6 - LOAD DATA TO BIGQUERY: PROGRAM END TIME: ${getCurrentDateTime()}; ELASPED TIME: ${elapsedTime} sec\n`;

//         console.log(`\n${message}\n`);
//         generateLogFile('scheduled_booking_data', `${message}\n`);
//         await slack_message_steve_calla_channel(message);

//         // NEXT STEP - No STEP #7
//         // await step_7(startTime);

//     } catch (error) {
//         console.error('Error executing Step #5:', error);
//         generateLogFile('scheduled_booking_data', `Error executing Step #4: ${error}`);
//         return; // Exit the function early
//     }
// }

async function createSlackMessage(results) {
    const goal = 325;
    const bookings_today = results[7].current_bookings;
    const bookings_yesterday = results[6].current_bookings;
    const today_above_below_goal = bookings_today - goal;
    const yesterday_above_below_goal = bookings_yesterday - goal;

    const created_at_date = `Info Updated at: ${results[0].created_at_gst}`;
    const most_recent_booking_date = `Most Recent Booking At: ${results[0].date_most_recent_created_on_gst}`;
    const goal_message = `Goal: ${goal}`;
    let booking_count_message = `Bookings: ${bookings_today}`;
    let today_above_below_goal_message = `Above / Below Goal: ${today_above_below_goal}`;
    
    let today_status = today_above_below_goal >= goal ? '✅🚀 Well done!' : '💪 Keep going!';
    let yesterday_status = `Yesterday: ${bookings_yesterday}, ${yesterday_above_below_goal} ${yesterday_above_below_goal >= 0 ? ' more than goal. Well done!🔥🔥' : 'less than goal. We’re learning from this! 👀'}`;
    
    slackMessage = `${created_at_date}\n${most_recent_booking_date}\n--------------\n${booking_count_message}\n${goal_message}\n${today_above_below_goal_message}\n${today_status}\n----------\n${yesterday_status}`;
    console.log(slackMessage);

    return slackMessage;
}

check_most_recent_created_on_date();
