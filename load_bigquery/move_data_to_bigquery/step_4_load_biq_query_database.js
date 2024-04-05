'use strict';

// Import the Google Cloud client libraries
const fs = require('fs').promises;
const { BigQuery } = require('@google-cloud/bigquery');
const { Storage } = require('@google-cloud/storage');

const BQ_CREDENTIALS_PATH = require('../auth_certs/cool-ship-418513-cadf086380e7_key2.json');
const { csvExportPath } = require('../../utilities/config');

const datasetId = "ezhire_metrics";
// const tableIds = ["booking_data", "key_metrics_data", "pacing_data"];
// const tableIds = ["key_metrics_data", "pacing_data"];
const tableIds = ["key_metrics_data",];
// const tableIds = ["pacing_data"];

// Import a GCS file into a table with manually defined schema.
async function execute_load_big_query_database() {
    const startTime = performance.now();
    let elapsedTime;
    
    // Instantiate clients
    const bigqueryClient = new BigQuery({ credentials: BQ_CREDENTIALS_PATH, });
    const storageClient = new Storage();
    
    /**
     * This sample loads the CSV file at
     * https://storage.googleapis.com/cloud-samples-data/bigquery/us-states/us-states.csv
    *
    * TODO(developer): Replace the following lines with the path to your file.
    */
    const directory = `${csvExportPath}bigquery`; // DIRECTORY CONTAINING CSV FILES
    const files = await fs.readdir(directory); // LIST ALL FILES IN THE DIRECTORY
    let numberOfFiles = 0;

    const bucketName = 'testing_bucket_v2';

    // Merge arrays into an object using map
    const merged_table_details = tableIds.map((table_name, index) => {
        return {
            tableName: table_name,
            tablePath: files[index],
        }
    });
    const filesLength = merged_table_details.length;

    // Imports a GCS file into a table with auto detect defined schema.

    for (const file of merged_table_details) {

        // Configure the load job. For full list of options, see:
        // https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfigurationLoad
        // source: https://cloud.google.com/bigquery/docs/samples/bigquery-load-table-gcs-csv-truncate
        const metadata = {
            sourceFormat: 'CSV',
            skipLeadingRows: 1,
            autodetect: true,
            location: 'US',
            // Set the write disposition to overwrite existing table data.
            writeDisposition: 'WRITE_TRUNCATE',
        };
    
        // Load data from a Google Cloud Storage file into the table
        const [job] = await bigqueryClient
            .dataset(datasetId)
            .table(file.tableName)
            .load(storageClient.bucket(bucketName).file(file.tablePath), metadata);
    
            
        const endTime = performance.now();
        elapsedTime = ((endTime - startTime) / 1000).toFixed(2); // CONVERT MS TO SEC
        
        // load() waits for the job to finish
        console.log(`File ${++numberOfFiles} of ${filesLength}, File name: ${file.tableName}`);
        console.log(`Job ${job.id} completed. Elapsed time: ${elapsedTime}\n`);
    
        // Check the job's status for errors
        const errors = job.status.errors;
        if (errors && errors.length > 0) {
            throw errors;
        }

    }

    const endTime = performance.now();
    elapsedTime = ((endTime - startTime) / 1000).toFixed(2); // CONVERT MS TO SEC
    return elapsedTime; // RETURN ELAPSED TIME AFTER ALL UPLOADS COMPLETE
}

module.exports = {
    execute_load_big_query_database,
}


