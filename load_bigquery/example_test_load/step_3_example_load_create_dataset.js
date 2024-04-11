'use strict';
const { BigQuery } = require('@google-cloud/bigquery'); // Import the Google Cloud client libraries

const BQ_CREDENTIALS_PATH = require('../auth_certs/cool-ship-418513-cadf086380e7_key2.json');
const datasetId = "my_states_dataset3";
const tableId = "my_states_table";

async function execute_create_dataset() {
    const startTime = performance.now();

    try {
        // Create a client with custom credentials
        const bigqueryClient = new BigQuery({
            credentials: BQ_CREDENTIALS_PATH,
        });

        // Create the dataset if it doesn't exist
        const [dataset] = await bigqueryClient.dataset(datasetId).get({ autoCreate: true });
        console.log(`Dataset ${dataset.id} created or already exists.`);

        const options = {
            location: 'US',
        };

        // Check if the table already exists
        const [tableExists] = await bigqueryClient
            .dataset(datasetId)
            .table(tableId)
            .exists();

        if (tableExists) {
            // Replace the existing table if it exists
            await bigqueryClient
                .dataset(datasetId)
                .table(tableId)
                .delete({ force: true }); // Delete the table with force option
        }

        // Create a new table in the dataset or replace the existing one
        const [table] = await bigqueryClient
            .dataset(datasetId)
            .createTable(tableId, {
                ...options,
                replace: true, // Replace the table if it already exists
            });


        // Create a new table in the dataset
        // const [table] = await bigqueryClient
        //     .dataset(datasetId)
        //     .createTable(tableId, options);

        console.log(`Table ${table.id} created.`);

        const endTime = performance.now();
        const elapsedTime = ((endTime - startTime) / 1_000).toFixed(2); //convert ms to sec
        return (elapsedTime); // Resolve the promise with the elapsed time

    } catch (error) {
        console.error('Error:', error);
        // reject(error); // Reject the promise if there's an error
    }
}

module.exports = {
    execute_create_dataset,
}
