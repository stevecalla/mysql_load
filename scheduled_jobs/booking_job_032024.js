const { generateLogFile } = require('../utilities/generateLogFile');

const { execute_get_booking_data } = require('../get_booking_data/sql_getBookingData_ssh_loop'); //step_1
const { execute_load_booking_data } = require('../load_booking_data/sql_load_bookingData'); //step_2
const { execute_create_key_metrics } = require('../create_keyMetrics_data/sql_getKeyMetrics_loop'); //step_3
const { execute_create_pacing_metrics } = require('../create_pacing_data/sql_getPacingMetrics_loop'); //step_4

async function get_booking_data() {
    try {
        // STEP #1: GET BOOKING DATA
        console.log('\n*************** STARTING STEP 1 ***************\n');
        await execute_get_booking_data();

        const message_1 = 'Command #1 retrieving booking data is complete';
        console.log(message_1);
        generateLogFile('scheduled booking data', message_1);
        await step_2();
    } catch (error) {
        console.error('Error executing Step #1:', error);
        generateLogFile('scheduled booking data', `Error executing Step #1: ${error}`);
        return; // Exit the function early
    }
}

async function step_2() {
    try {
        // STEP #2: LOAD BOOKING DATA
        console.log('\n*************** STARTING STEP 2 ***************\n');
        await execute_load_booking_data();

        const message_2 = 'Command #2 loading booking data is complete';
        console.log(message_2);
        generateLogFile('scheduled booking data', message_2);

        await step_3();
    } catch (error) {
        console.error('Error executing Step #2:', error);
        generateLogFile('scheduled booking data', `Error executing Step #2: ${error}`);
        return; // Exit the function early
    }
}

async function step_3() {
    try {
        // STEP #3: CREATE KEY METRICS / ON RENT DATA
        console.log('\n*************** STARTING STEP 3 ***************\n');
        await execute_create_key_metrics();

        const message_3 = 'Command #3 creating key metrics data is complete';
        console.log(message_3);
        generateLogFile('scheduled booking data', message_3);

        await step_4();
    } catch (error) {
        console.error('Error executing Step #3:', error);
        generateLogFile('scheduled booking data', `Error executing Step #3: ${error}`);
        return; // Exit the function early
    }
}

async function step_4() {
    try {
        // STEP #4: CREATE PACING DATA
        console.log('\n*************** STARTING STEP 4 ***************\n');
        await execute_create_pacing_metrics();

        const message_4 = 'Command #4 creating pacing data is comNlete';
        console.log(message_4);
        generateLogFile('scheduled booking data', message_4);
    } catch (error) {
        console.error('Error executing Step #4:', error);
        generateLogFile('scheduled booking data', `Error executing Step #4: ${error}`);
        return; // Exit the function early
    }
}

get_booking_data();
