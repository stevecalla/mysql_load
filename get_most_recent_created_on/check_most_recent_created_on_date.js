const { getCurrentDateTime } = require('../utilities/getCurrentDate');

const { slack_message_drissues_channel } = require('../schedule_slack/slack_drissues_channel');
const { slack_message_steve_calla_channel } = require('../schedule_slack/slack_steve_calla_channel');
// const { slack_message_400_bookings_channel } = require('../schedule_slack/slack_400_bookings_channel');
// const { create_daily_booking_slack_message } = require('../schedule_slack/slack_daily_booking_message');

const { execute_get_most_recent_created_on_date } = require('./sql_getBookingMostRecentCreatedOn'); //step_0

// TESTING VARIABLES
// const is_testing = false;

// RUN PROGRAM
let run_step_0 = true;     // get most recent created on / updated on datetime

// STEP #0: RUN QUERY TO GET MOST RECENT CREATED ON / UPDATED ON DATE
async function check_most_recent_created_on_date(is_testing = false) {
    const start_time = performance.now();
    const step = `STEP 0 - CHECK MOST RECENT DATE. `;

    await program_start_message(step);

    let is_development_pool = true;

    try {
        if (run_step_0) {
            // EXECUTE QUERIES
            let getResults = await execute_get_most_recent_created_on_date();
            console.log('query results = ', getResults);

            const results = getResults.results[0];
            let { is_within_15_minutes, is_within_2_hours } = results;

            console.log('is with 15 = ', is_within_15_minutes, typeof is_within_15_minutes, !is_within_15_minutes);
            console.log('is with 2hr = ', is_within_2_hours, typeof is_within_2_hours, !is_within_2_hours);

            // // Assign false to is_within_15_minutes if is_testing is true
            // if (is_testing) {
            //     is_within_15_minutes = false;
            // }

            // // Assign false to is_within_2_hours if is_testing is true
            // if (is_testing) {
            //     is_within_2_hours = false;
            // }

            if (!is_within_15_minutes) {
                // change to production db pool connection
                is_development_pool = false;
            }
            
            // slack messages
            let { slack_message_15_minutes, slack_message_2_hours } = await create_slack_message(getResults, is_testing);

            is_within_2_hours ? await slack_message_steve_calla_channel(slack_message_15_minutes) : await slack_message_steve_calla_channel(slack_message_2_hours);

            !is_testing && !is_within_2_hours && await slack_message_drissues_channel(slack_message_2_hours);

        } else {
            await program_skip_message(step);   
        }

    } catch (error) {
        console.error(`Error executing ${step}`, error);
        return; // Exit the function early
    } finally {
        
        await program_end_message(start_time, step);

        return { start_time, is_development_pool };
    }
}

// MESSAGES
async function program_start_message(step) {
    console.log(`\n*************** STARTING ${step} ***************`);
    console.log(`PROGRAM START TIME = ${getCurrentDateTime()}`);
}

async function program_skip_message(step) {
    console.log(`\nSKIPPED ${step} DUE TO TOGGLE SET TO FALSE.`);
}

async function program_end_message(start_time, step) {
    const end_time = performance.now();
    const elapsed_time = ((end_time - start_time) / 1_000).toFixed(2); //convert ms to sec

    console.log(`\nPROGRAM END TIME: ${getCurrentDateTime()}; ELASPED TIME: ${elapsed_time} sec`);
    console.log(`*************** END OF ${step} ***************`);
}

async function create_slack_message(data, is_testing) {
    // log message
    let log_message = await create_log_message(data);

    // slack mesage
    results = data.results[0];
    let { source_field, time_stamp_difference_minute, time_stamp_difference_hour, is_within_15_minutes, is_within_2_hours } = results;
    let elapsed_time = `QUERY ELAPSED TIME: ${data.elapsedTime}`;

    // // Assign false to is_within_15_minutes if is_testing is true
    // if (is_testing) {
    //     is_within_15_minutes = false;
    // }

    // // Assign false to is_within_2_hours if is_testing is true
    // if (is_testing) {
    //     is_within_2_hours = false;
    // }

    let inside_15_minutes = `WITHIN 15 MINUTES USING MOST RECENT ${source_field.toUpperCase()}.\nUSING DR DEVELOPMENT DB`;
    let outside_15_minutes =  `OUTSIDE 15 MINUTES USING MOST RECENT ${source_field.toUpperCase()}.\nUSING PRODUCTION DB`;
    let minutes_diff_message = `TIME STAMP DIFFERENCE - MINUTES: ${time_stamp_difference_minute}`;
    
    let slack_message_15_minutes = (is_within_15_minutes ? 
        `\n${inside_15_minutes}\n${minutes_diff_message}\n${elapsed_time}\n${log_message}` : 
        `\n${outside_15_minutes}\n${minutes_diff_message}\n${elapsed_time}\n${log_message}`);
    
    let inside_2_hours = `WITHIN 2 HOURS USING MOST RECENT ${source_field.toUpperCase()}.\nUSING DR DEVELOPMENT DB`;
    let outside_2_hours = `OUTSIDE 2 HOURS USING MOST RECENT ${source_field.toUpperCase()}.\nUSING PRODUCTION DB`;
    let hours_diff_message = `TIME STAMP DIFFERENCE - HOURS: ${time_stamp_difference_hour}`;
    
    let slack_message_2_hours = is_within_2_hours ? 
        `\n${inside_2_hours}\n${hours_diff_message}\n${elapsed_time}\n${log_message}` : 
        `\n${outside_2_hours}\n${hours_diff_message}\n${elapsed_time}\n${log_message}`;

    return {slack_message_15_minutes, slack_message_2_hours};
}

async function create_log_message(data) {
    results = data.results[0];

    let { source_field, most_recent_event_update_gst, execution_timestamp_gst, time_stamp_difference_minute, is_within_15_minutes } = results;

    let log_message = results ? `\nEXECUTION TIMESTAMP: ${execution_timestamp_gst}\nLAST UPDATED: ${most_recent_event_update_gst}\nTIME STAMP DIFFERENCE - MINUTES: ${time_stamp_difference_minute}\nSOURCE FIELD: ${source_field}\nIS WITHIN 15 MINUTES: ${is_within_15_minutes}` : `Opps no results`;

    return log_message;
}

// check_most_recent_created_on_date();

module.exports = {
    check_most_recent_created_on_date,
}