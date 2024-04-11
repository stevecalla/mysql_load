const { generateLogFile } = require('../../utilities/generateLogFile');
const { getCurrentDateTime } = require('../../utilities/getCurrentDate');

const { execute_create_csv_file } = require('../../utilities/json_to_csv'); //step_1
const { execute_load_data_to_cloud } = require('./step_2_example_load_upload_to_cloud'); //step_2
const { execute_create_dataset } = require('./step_3_example_load_create_dataset'); //step_3
const { execute_load_csv_to_big_query } = require('./step_4_example_load_biq_query_csv'); //step_4

const run_step_1 = true; // convert file to csv
const run_step_2 = true; // load csv to google cloud bucket
const run_step_3 = true; // create_dataset_table
const run_step_4 = true; // load csv file to bigquery

async function load_data_to_bigquery() {
    const startTime = performance.now();
    console.log(`\n\nPROGRAM START TIME = ${getCurrentDateTime()}`);
    generateLogFile('load_cloud_data', `\n\nPROGRAM START TIME = ${getCurrentDateTime()}`);

    try {
        // STEP #1: CONVERT FILE TO CSV
        console.log('\n*************** STARTING STEP 1 ***************\n');

        if (run_step_1) {
            // EXECUTE QUERIES
            let getResults;
            getResults = await execute_create_csv_file();
    
            // LOGS
            let message = getResults ? `\nCreate csv from json executed successfully. Elapsed Time: ${getResults}`: `Opps error getting elapsed time\n`;

            console.log(message);
            generateLogFile('load_cloud_data', message);
 
        } else {
            // LOGS
            let message = `\nSkipped STEP 1 due to toggle set to false.\n`;
            console.log(message);
            generateLogFile('load_cloud_data', message); 
        }
        
        console.log('\n*************** END OF STEP 1 ***************\n');
        // NEXT STEP
        await step_2(startTime);
    } catch (error) {
        console.error('Error executing Step #1:', error);
        generateLogFile('load_cloud_data', `Error executing Step #1: ${error}`);
        return; // Exit the function early
    }
}

async function step_2(startTime) {
    try {
        // STEP #2: load csv to google cloud bucket
        console.log('\n*************** STARTING STEP 2 ***************\n');

        if (run_step_2) {
            // EXECUTE QUERIES
            let getResults;
            getResults = await execute_load_data_to_cloud();
    
            // LOGS
            let message = getResults ? `\nLoad csv to google cloud bucket executed successfully. Elapsed Time: ${getResults}` : `Opps error getting elapsed time\n`;

            console.log(message);
            generateLogFile('load_cloud_data', message);
    
        } else {
            // LOGS
            let message = `\nSkipped STEP 2 due to toggle set to false.\n`;
            console.log(message);
            generateLogFile('load_cloud_data', message);   
        }
        
        console.log('\n*************** END OF STEP 2 ***************\n');
        // NEXT STEP
        await step_3(startTime);
    } catch (error) {
        console.error('Error executing Step #2:', error);
        generateLogFile('load_cloud_data', `Error executing Step #2: ${error}`);
        return; // Exit the function early
    }
}

async function step_3(startTime) {
    try {
        // STEP #3: create_dataset_table
        console.log('\n*************** STARTING STEP 3 ***************\n');

        if (run_step_3) {
            // EXECUTE QUERIES
            let getResults;
            getResults = await execute_create_dataset();
    
            // LOGS
            let message = getResults ? `\nCreate dataset table executed successfully. Elapsed Time: ${getResults}` : `Opps error getting elapsed time\n`;

            console.log(message);
            generateLogFile('load_cloud_data', message);
    
            
        } else {
            // LOGS
            let message = `\nSkipped STEP 3 due to toggle set to false.\n`;
            console.log(message);
            generateLogFile('load_cloud_data', message);
        }
        
        console.log('\n*************** END OF STEP 3 ***************\n');
        // NEXT STEP
        await step_4(startTime);
    } catch (error) {
        console.error('Error executing Step #3:', error);
        generateLogFile('load_cloud_data', `Error executing Step #3: ${error}`);
        return; // Exit the function early
    }
}

async function step_4(startTime) {

    try {
        // STEP #4: load csv file to bigquery
        console.log('\n*************** STARTING STEP 4 ***************\n');

        if (run_step_4) {
            // EXECUTE QUERIES
            let getResults;
            getResults = await execute_load_csv_to_big_query();
    
            // LOGS
            let message = getResults ? `\nAll create pacing data queries executed successfully. Elapsed Time: ${getResults}` : `Opps error getting elapsed time\n`;

            console.log(message);
            console.log('\n*************** END OF STEP 4 ***************\n');
            generateLogFile('load_cloud_data', message);
    
        } else {
            // LOGS
            let message = `\nSkipped STEP 4 due to toggle set to false.\n`;

            console.log(message);
            generateLogFile('load_cloud_data', message);
        }

        
        console.log('\n*************** END OF STEP 4 ***************\n');
        //NEXT STEP = NONE

        const endTime = performance.now();
        const elapsedTime = ((endTime - startTime) / 1_000).toFixed(2); //convert ms to sec
        console.log(`\nPROGRAM END TIME: ${getCurrentDateTime()}; ELASPED TIME: ${elapsedTime} sec\n`);
        generateLogFile('load_cloud_data', `\nPROGRAM END TIME: ${getCurrentDateTime()}; ELASPED TIME: ${elapsedTime} sec\n`);

    } catch (error) {
        console.error('Error executing Step #4:', error);
        generateLogFile('load_cloud_data', `Error executing Step #4: ${error}`);
        return; // Exit the function early
    }
}

load_data_to_bigquery();
