const { generateLogFile } = require('../utilities/generateLogFile');

const { execute_get_booking_data } = require('../get_booking_data/sql_getBookingData_ssh_loop'); //step_1
const { execute_load_booking_data } = require('../load_booking_data/sql_load_bookingData'); //step_2
const { execute_create_key_metrics } = require('../create_keyMetrics_data/sql_getKeyMetrics_loop'); //step_3
const { execute_create_pacing_metrics } = require('../create_pacing_data/sql_getPacingMetrics_loop'); //step_4

const run_step_1 = true; // get booking data
const run_step_2 = true; // load booking data
const run_step_3 = true; // create key metrics
const run_step_4 = true; // create pacing metrics

async function get_booking_data() {
    try {
        // STEP #1: GET BOOKING DATA
        console.log('\n*************** STARTING STEP 1 ***************\n');
        // EXECUTE QUERIES
        let getResults;
        run_step_1 && (getResults = await execute_get_booking_data());

        // LOGS
        const message = `\nAll get booking data queries executed successfully. Elapsed Time: ${getResults ? getResults : "Opps error getting time"} sec\n`;
        console.log(message);
        console.log('\n*************** END OF STEP 1 ***************\n');
        generateLogFile('scheduled booking data', message);

        // NEXT STEP
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
        // EXECUTE QUERIES
        let getResults;
        run_step_2 && (getResults = await execute_load_booking_data());

        // LOGS
        const message = `\nAll loading data queries executed successfully. Elapsed Time: ${getResults ? getResults : "Opps error getting time"} sec\n`;
        console.log(message);
        console.log('\n*************** END OF STEP 2 ***************\n');
        generateLogFile('scheduled booking data', message);

        // NEXT STEP
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
        // EXECUTE QUERIES
        let getResults;
        run_step_3 && (getResults = await execute_create_key_metrics());

        // LOGS
        const message = `\nAll create key metrics queries executed successfully. Elapsed Time: ${getResults ? getResults : "Opps error getting time"} sec\n`;
        console.log(message);
        console.log('\n*************** END OF STEP 3 ***************\n');
        generateLogFile('scheduled booking data', message);

        // NEXT STEP
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
        // EXECUTE QUERIES
        let getResults;
        run_step_4 && (getResults = await execute_create_pacing_metrics());

        // LOGS
        const message = `\nAll create pacing data queries executed successfully. Elapsed Time: ${getResults ? getResults : "Opps error getting time"} sec\n`;
        console.log(message);
        console.log('\n*************** END OF STEP 4 ***************\n');
        generateLogFile('scheduled booking data', message);

        //NEXT STEP = NONE
    } catch (error) {
        console.error('Error executing Step #4:', error);
        generateLogFile('scheduled booking data', `Error executing Step #4: ${error}`);
        return; // Exit the function early
    }
}

get_booking_data();
