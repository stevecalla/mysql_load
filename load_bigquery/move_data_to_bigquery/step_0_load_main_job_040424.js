const { generateLogFile } = require('../../utilities/generateLogFile');
const { getCurrentDateTime } = require('../../utilities/getCurrentDate');

const { execute_retrieve_data } = require('./step_1_retrieve_data_process'); //step_1
const { execute_upload_csv_to_cloud } = require('./step_2_upload_csv_to_cloud'); //step_2
const { execute_create_bigquery_dataset } = require('./step_3_create_bigquery_dataset'); //step_3
const { execute_load_big_query_database } = require('./step_4_load_biq_query_database'); //step_4

//TODO:
const run_step_1 = true; // retrieve booking, key metrics, pacing data
const run_step_2 = true; // load csv to google cloud bucket
const run_step_3 = true; // create_dataset_table
const run_step_4 = true; // load csv file to bigquery

// STEP #1: RETRIEVE BOOKING, KEY METRICS AND PACING DATA
async function execute_load_data_to_bigquery() {
    const startTime = performance.now();
    console.log(`\n\nPROGRAM START TIME = ${getCurrentDateTime()}`);
    generateLogFile('load_big_query', `\n\nPROGRAM START TIME = ${getCurrentDateTime()}`);

    try {
        // STEP #1: RETRIEVE BOOKING, KEY METRICS AND PACING DATA
        console.log('\n*************** STARTING STEP 1 ***************\n');

        if (run_step_1) {
            // EXECUTE QUERIES
            let getResults;
            getResults = await execute_retrieve_data();
    
            // LOGS
            let message = getResults ? `\nRetrieve booking, key metric, pacing data successfully. Elapsed Time: ${getResults}`: `Opps error getting elapsed time\n`;

            console.log(message);
            generateLogFile('load_big_query', message);
 
        } else {
            // LOGS
            let message = `\nSkipped STEP 1 due to toggle set to false.\n`;
            console.log(message);
            generateLogFile('load_big_query', message); 
        }
        
        console.log('\n*************** END OF STEP 1 ***************\n');
        // NEXT STEP
        await step_2(startTime);
    } catch (error) {
        console.error('Error executing Step #1:', error);
        generateLogFile('load_big_query', `Error executing Step #1: ${error}`);
        return; // Exit the function early
    }
}

// STEP #2: load csv to google cloud bucket
async function step_2(startTime) {
    try {
        console.log('\n*************** STARTING STEP 2 ***************\n');

        if (run_step_2) {
            // EXECUTE QUERIES
            let getResults;
            getResults = await execute_upload_csv_to_cloud();
    
            // LOGS
            let message = getResults ? `\nLoad csv to google cloud bucket executed successfully. Elapsed Time: ${getResults}` : `Opps error getting elapsed time\n`;

            console.log(message);
            generateLogFile('load_big_query', message);
    
        } else {
            // LOGS
            let message = `\nSkipped STEP 2 due to toggle set to false.\n`;
            console.log(message);
            generateLogFile('load_big_query', message);   
        }
        
        console.log('\n*************** END OF STEP 2 ***************\n');
        // NEXT STEP
        await step_3(startTime);
    } catch (error) {
        console.error('Error executing Step #2:', error);
        generateLogFile('load_big_query', `Error executing Step #2: ${error}`);
        return; // Exit the function early
    }
}

// STEP #3: create_dataset_table
async function step_3(startTime) {
    try {
        console.log('\n*************** STARTING STEP 3 ***************\n');

        if (run_step_3) {
            // EXECUTE QUERIES
            let getResults;
            getResults = await execute_create_bigquery_dataset();
    
            // LOGS
            let message = getResults ? `\nCreate bigquery dataset executed successfully. Elapsed Time: ${getResults}` : `Opps error getting elapsed time\n`;

            console.log(message);
            generateLogFile('load_big_query', message);
    
            
        } else {
            // LOGS
            let message = `\nSkipped STEP 3 due to toggle set to false.\n`;
            console.log(message);
            generateLogFile('load_big_query', message);
        }
        
        console.log('\n*************** END OF STEP 3 ***************\n');
        // NEXT STEP
        await step_4(startTime);
    } catch (error) {
        console.error('Error executing Step #3:', error);
        generateLogFile('load_big_query', `Error executing Step #3: ${error}`);
        return; // Exit the function early
    }
}

// STEP #4: load csv file to bigquery
async function step_4(startTime) {
    try {
        console.log('\n*************** STARTING STEP 4 ***************\n');

        if (run_step_4) {
            // EXECUTE QUERIES
            let getResults;
            getResults = await execute_load_big_query_database();
    
            // LOGS
            let message = getResults ? `Load csv to bigquery executed successfully. Elapsed Time: ${getResults}` : `Opps error getting elapsed time\n`;

            console.log(message);
            generateLogFile('load_big_query', message);
            
        } else {
            // LOGS
            let message = `\nSkipped STEP 4 due to toggle set to false.\n`;
            
            console.log(message);
            console.log('\n*************** END OF STEP 4 ***************\n');
            generateLogFile('load_big_query', message);
        }
        
        const endTime = performance.now();
        const elapsedTime = ((endTime - startTime) / 1_000).toFixed(2); //convert ms to sec

        console.log(`\nPROGRAM END TIME: ${getCurrentDateTime()}; ELASPED TIME: ${elapsedTime} sec\n`);

        generateLogFile('load_big_query', `\nPROGRAM END TIME: ${getCurrentDateTime()}; ELASPED TIME: ${elapsedTime} sec\n`);

        console.log('*************** END OF STEP 4 ***************\n');
        
        //NEXT STEP = NONE

    } catch (error) {
        console.error('Error executing Step #4:', error);
        generateLogFile('load_big_query', `Error executing Step #4: ${error}`);
        return; // Exit the function early
    }
}

// execute_load_data_to_bigquery();

module.exports = {
    execute_load_data_to_bigquery,
}