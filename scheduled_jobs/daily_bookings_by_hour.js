const { generateLogFile } = require('../utilities/generateLogFile');
const { getCurrentDateTime } = require('../utilities/getCurrentDate');

const { slack_message_drissues_channel } = require('../schedule_slack/slack_drissues_channel');
const { slack_message_steve_calla_channel } = require('../schedule_slack/slack_steve_calla_channel');
const { slack_message_325_bookings_channel } = require('../schedule_slack/slack_325_bookings_channel');

const { execute_get_most_recent_created_on_date } = require('../get_booking_data/sql_getBookingMostRecentCreatedOn'); //step_0
const { execute_get_daily_booking_data } = require('../daily_booking_forecast/step_1_sql_get_daily_booking_data'); //step_1

let run_step_0 = true;     // get most recent created on / updated on datetime
let run_step_1 = true;     // get daily booking data

async function check_most_recent_created_on_date() {
    const startTime = performance.now();
    console.log(`\n\nPROGRAM START TIME = ${getCurrentDateTime()}`);
    generateLogFile('daily_booking_data', `\n\nPROGRAM START TIME = ${getCurrentDateTime()}`);

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

                // (b) LOGS
                let fail_message = getResults ? `\nHello - Myproject db needs some attention please. DB rental_car_booking2 most recent created on time is outside 2 hours. Elapsed Time: ${getResults.elapsedTime}` : `Opps error getting elapsed time`;

                // (c) send slack with warning
                slackMessage = `${fail_message}\n${log_results}`;
                await slack_message_drissues_channel(slackMessage);
                // await slack_message_steve_calla_channel(slackMessage);

                console.log(fail_message);
                console.log(getResults);

                generateLogFile('daily_booking_data', fail_message);
                generateLogFile('daily_booking_data', log_results);

                const endTime = performance.now();
                const elapsedTime = ((endTime - startTime) / 1_000).toFixed(2); //convert ms to sec
                console.log(`\nPROGRAM END TIME: ${getCurrentDateTime()}; ELASPED TIME: ${elapsedTime} sec\n`);
                generateLogFile('daily_booking_data', `\nPROGRAM END TIME: ${getCurrentDateTime()}; ELASPED TIME: ${elapsedTime} sec\n`);

                // (c) return to exit function
                return;
            }

            // LOGS
            success_message = getResults ? `\nMost recent created on time is within 2 hours. Elapsed Time: ${getResults.elapsedTime}` : `Opps error getting elapsed time\n`;

            console.log(success_message);
            generateLogFile('daily_booking_data', success_message);

        } else {
            // LOGS
            let skip_message = `\nSkipped STEP 1 due to toggle set to false.\n`;
            console.log(skip_message);
            generateLogFile('daily_booking_data', skip_message);
        }

        console.log('\n*************** END OF STEP 1 ***************\n');

        // LOGS & SLACK MESSAGE
        const endTime = performance.now();
        const elapsedTime = ((endTime - startTime) / 1_000).toFixed(2); //convert ms to sec

        let message = `\nSTEP 0 - CHECK MOST RECENT DATE: PROGRAM END TIME: ${getCurrentDateTime()}; ELASPED TIME: ${elapsedTime} sec\n`;

        console.log(`\n${message}\n`);
        generateLogFile('daily_booking_data', `${message}\n`);

        slackMessage = `${message}${success_message}\n${log_results}`;
        await slack_message_steve_calla_channel(slackMessage);

        // NEXT STEP
        await step_1_get_daily_booking_data(startTime);

    } catch (error) {
        console.error('Error executing Step #1:', error);
        generateLogFile('daily_booking_data', `Error executing Step #1: ${error}`);
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
            let message = getResults ? `\nAll get booking data queries executed successfully.` : `Opps error getting elapsed time\n`;

            console.log(message);
            generateLogFile('daily_booking_data', message);

        } else {
            // LOGS
            let message = `\nSkipped STEP 1 due to toggle set to false.\n`;
            console.log(message);
            generateLogFile('daily_booking_data', message);
        }

        console.log('\n*************** END OF STEP 1 ***************\n');

        // LOGS & SLACK MESSAGE
        const endTime = performance.now();
        const elapsedTime = ((endTime - startTime) / 1_000).toFixed(2); //convert ms to sec

        let message = `\nSTEP 1 - GET DAILY BOOKING DATA: PROGRAM END TIME: ${getCurrentDateTime()}; ELASPED TIME: ${elapsedTime} sec\n`;

        console.log(`\n${message}\n`);
        generateLogFile('daily_booking_data', `${message}\n`);

        if (getResults) {
            const slackMessage = await createSlackMessage(getResults);
            // await slack_message_steve_calla_channel(slackMessage);
            await slack_message_325_bookings_channel(slackMessage);
            console.log('step_1_get_daily_booking_data = ', slackMessage);
        }
        process.exit(0)

        // NEXT STEP
        // await step_2(startTime);

    } catch (error) {
        console.error('Error executing Step #1:', error);
        generateLogFile('daily_booking_data', `Error executing Step #1: ${error}`);
        process.exit(1); // Exit on error
        // return; // Exit the function early
    }
}

async function createSlackMessage(results) {
    const goal = 325;
    const bookings_today = results[7].current_bookings;
    const bookings_yesterday = results[6].current_bookings;
    const today_above_below_goal = bookings_today - goal;
    const yesterday_above_below_goal = bookings_yesterday - goal;

    const created_at_date = `Info Updated At: ${results[0].created_at_gst} GST`;
    const most_recent_booking_date = `Most Recent Booking At: ${results[0].date_most_recent_created_on_gst} GST`;
    let booking_count_message = `📢Bookings: ${bookings_today}`;
    const goal_message = `🎯Goal: ${goal}`;
    let today_above_below_goal_message = `${today_above_below_goal >= 0 ? `🟢Goal: ${today_above_below_goal}` : `🔴Below Goal: ${today_above_below_goal}`}`;
    let pacing_thresholds = `Pacing Goals: 10a-11a = 60, noon-1p = 100, 4p-5p = 200, 8p-10p = 300`

    let today_status = today_above_below_goal >= goal ? '✅🚀Well done!' : '💪Keep going!';
    let yesterday_status = `Yesterday: ${yesterday_above_below_goal >= 0 ? `Well done🔥🔥 = ${bookings_yesterday}!! Bookings were +${yesterday_above_below_goal} above goal.` : `We’re learning from this! 👀. Bookings were ${yesterday_above_below_goal} below goal.`}`;

    const slackMessage = `\n**************\nUAE ONLY\n${created_at_date}\n${most_recent_booking_date}\n--------------\n${booking_count_message}\n${goal_message}\n${today_above_below_goal_message}\n${today_status}\n--------------\n${pacing_thresholds}\n--------------\n${yesterday_status}\n**************\n`;
    // console.log(slackMessage);

    return slackMessage;
}

check_most_recent_created_on_date();