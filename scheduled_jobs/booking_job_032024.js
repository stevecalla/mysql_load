const { generateLogFile } = require('../utilities/generateLogFile');
const { getCurrentDateTime } = require('../utilities/getCurrentDate');

const { execute_get_booking_data } = require('../get_booking_data/sql_getBookingData_ssh_loop'); //step_1
const { execute_load_booking_data } = require('../load_booking_data/sql_load_bookingData'); //step_2
const { execute_create_key_metrics } = require('../create_keyMetrics_data/sql_getKeyMetrics_loop'); //step_3
const { execute_create_pacing_metrics } = require('../create_pacing_data/sql_getPacingMetrics_loop'); //step_4

const run_step_1 = true; // get booking data
const run_step_2 = true; // load booking data
const run_step_3 = true; // create key metrics
const run_step_4 = true; // create pacing metrics   

async function get_booking_data() {
    const startTime = performance.now();
    console.log(`\n\nPROGRAM START TIME = ${getCurrentDateTime()}`);
    generateLogFile('scheduled_booking_data', `\n\nPROGRAM START TIME = ${getCurrentDateTime()}`);

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
        //NEXT STEP = NONE

        const endTime = performance.now();
        const elapsedTime = ((endTime - startTime) / 1_000).toFixed(2); //convert ms to sec
        console.log(`\nPROGRAM END TIME: ${getCurrentDateTime()}; ELASPED TIME: ${elapsedTime} sec\n`);
        generateLogFile('scheduled_booking_data', `\nPROGRAM END TIME: ${getCurrentDateTime()}; ELASPED TIME: ${elapsedTime} sec\n`);

    } catch (error) {
        console.error('Error executing Step #4:', error);
        generateLogFile('scheduled_booking_data', `Error executing Step #4: ${error}`);
        return; // Exit the function early
    }
}

get_booking_data();
