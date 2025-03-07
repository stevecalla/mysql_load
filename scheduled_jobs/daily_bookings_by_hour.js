const dotenv = require('dotenv');
dotenv.config({ path: "../../.env" }); // adding the path ensures each folder will read the .env file as necessary

const { getCurrentDateTime } = require('../utilities/getCurrentDate');

const { slack_message_steve_calla_channel } = require('../schedule_slack/slack_steve_calla_channel');
const { slack_message_400_bookings_channel } = require('../schedule_slack/slack_400_bookings_channel');
const { slack_message_350_bookings_channel } = require('../schedule_slack/slack_350_bookings_channel');
const { create_daily_booking_slack_message } = require('../schedule_slack/slack_daily_booking_message');

const { check_most_recent_created_on_date } = require('../get_most_recent_created_on/check_most_recent_created_on_date'); //step_0
const { execute_get_daily_booking_data } = require('../daily_booking_data/step_1_sql_get_daily_booking_data'); //step_1
const { execute_get_car_availability } = require('../daily_car_availability_data/step_1_sql_get_car_availability');
const { execute_get_slack_forecast_data } = require('../daily_booking_forecast/step_3_get_slack_forecast_data');

// TESTING VARIABLES
let send_slack_to_calla = false;
let is_testing = false; // test is_within_15_minutes in check_most_recent_created_on_date.js

// RUN PROGRAM
let run_step_0 = true;     // get most recent created on / updated on datetime
let run_step_1 = true;     // get daily booking data

// STEP #0: RUN QUERY TO GET MOST RECENT CREATED ON / UPDATED ON DATE
async function run_most_recent_check() {
    const step = `STEP #0: RUN QUERY TO GET MOST RECENT CREATED ON / UPDATED ON DATE `;
    let start_time = performance.now();

    let is_development_pool = true;
    
    try {
        if (run_step_0) {

            let result = await check_most_recent_created_on_date(is_testing); // USE DR DB OR PRODUCTION DB

            console.log(result);

            is_development_pool = result.is_development_pool;

            // is_development_pool = false; // switch to production if necessary
            
            start_time = result.start_time;

        } else {
            await program_skip_message(step);   
        }

    } catch (error) {

        console.error(`Error executing ${step}`, error);
        return; // Exit the function early

    } finally {

        // NEXT STEP
        await step_1_get_daily_booking_data(start_time, is_development_pool);
    }
}

// STEP #1: GET DAILY BOOKING DATA / POST BOOKING DATA TO SLACK 400 CHANNEL
async function step_1_get_daily_booking_data(start_time, is_development_pool) {
    const step = `STEP 1 - GET DAILY BOOKING DATA. `;

    console.log('get daily bookings step 1 = ', is_development_pool);
    
    await program_start_message(step);

    try {
        if (run_step_1) {

            // EXECUTE QUERY
            const getBookingData = await execute_get_daily_booking_data(is_development_pool);
            // console.table(getBookingData);
            const getCarAvailability = await execute_get_car_availability(is_development_pool);
            // console.table(getCarAvailability);
            const getForecastData = await execute_get_slack_forecast_data(is_development_pool);
            console.table(getForecastData);

            // LOGS
            const booking_message = getBookingData ? `\nGet booking data queries executed successfully.` : `Opps error getting booking data\n`;
            console.log(booking_message);

            const car_message = getCarAvailability ? `\nGet car data queries executed successfully.` : `Opps error getting car data\n`;
            console.log(car_message);

            const forecast_message = getForecastData ? `\nGet forecast data queries executed successfully.` : `Opps error getting forecast data\n`;
            console.log(forecast_message);

            if (getBookingData) {

                const slack_message = await create_daily_booking_slack_message(getBookingData, getCarAvailability, getForecastData);

                if (send_slack_to_calla) {
                    await slack_message_steve_calla_channel(slack_message);
                  } else {
                    await slack_message_400_bookings_channel(slack_message);
                    await slack_message_350_bookings_channel(slack_message);
                  }

                // console.log(step, slack_message);
            };

        } else {
            await program_skip_message(step);
        }

        await program_end_message(start_time, step);

    } catch (error) {
        console.error(`Error executing ${step}`, error);
        // process.exit(1); // Exit on error
        // return;
    } finally {
        // NEXT STEP
        // await step_2(start_time);
        // process.exit(0);
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

// run_most_recent_check(); // need to run like this for bat file execution

module.exports = {
    run_most_recent_check,
}