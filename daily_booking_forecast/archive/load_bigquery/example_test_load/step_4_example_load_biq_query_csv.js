'use strict';

// Import the Google Cloud client libraries
const { BigQuery } = require('@google-cloud/bigquery');
const { Storage } = require('@google-cloud/storage');

const dotenv = require('dotenv');
dotenv.config({ path: "../../.env" }); // add path to read.env file

// const BQ_CREDENTIALS_PATH = require('../auth_certs/cool-ship-418513-cadf086380e7_key2.json');
const GOOGLE_SERVICE_ACCOUNT = JSON.parse(process.env.GOOGLE_SERVICE_ACCOUNT);
const datasetId = "my_states_dataset3";
const tableId = "my_states_table";

// Import a GCS file into a table with manually defined schema.
async function execute_load_csv_to_big_query() {
    // Instantiate clients
    const bigqueryClient = new BigQuery({ credentials: GOOGLE_SERVICE_ACCOUNT, });
    const storageClient = new Storage();

    /**
    * This sample loads the CSV file at
    * https://storage.googleapis.com/cloud-samples-data/bigquery/us-states/us-states.csv
    *
    * TODO(developer): Replace the following lines with the path to your file.
    */
    const bucketName = 'testing_bucket_v2';
    const filename = 'example_test_csv_file.csv';

    // Imports a GCS file into a table with manually defined schema.

    // Configure the load job. For full list of options, see:
    // https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfigurationLoad
    const metadata = {
        sourceFormat: 'CSV',
        skipLeadingRows: 1,
        schema: {
            fields: [
                { name: 'name', type: 'STRING' },
                { name: 'post_abbr', type: 'STRING' },
            ],
        },
        location: 'US',
    };

    // Load data from a Google Cloud Storage file into the table
    const [job] = await bigqueryClient
        .dataset(datasetId)
        .table(tableId)
        .load(storageClient.bucket(bucketName).file(filename), metadata);

    // load() waits for the job to finish
    console.log(`Job ${job.id} completed.`);

    // Check the job's status for errors
    const errors = job.status.errors;
    if (errors && errors.length > 0) {
        throw errors;
    }
}

// createDataset(datasetId);
// createTable(datasetId, tableId);
// loadJSONFromGCS(datasetId, tableId);
// }

module.exports = {
    execute_load_csv_to_big_query,
}


