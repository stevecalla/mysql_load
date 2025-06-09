'use strict';

// Import the Google Cloud client library
const { BigQuery } = require('@google-cloud/bigquery');
const BQ_CREDENTIALS_PATH = require('../auth_certs/cool-ship-418513-cadf086380e7_key2.json');
const { convertTimestampToDateTime } = require('../../utilities/getCurrentDate');

function main() {
    async function queryShakespeareDisableCache() {
        // Queries the Shakespeare dataset with the cache disabled.

        // Create a client with custom credentials
        const bigqueryClient = new BigQuery({
            credentials: BQ_CREDENTIALS_PATH,
        });

        const sqlQuery = `SELECT word, word_count
            FROM \`bigquery-public-data.samples.shakespeare\`
            WHERE corpus = @corpus
            AND word_count >= @min_word_count
            ORDER BY word_count DESC`;

        const options = {
            query: sqlQuery,
            // Location must match that of the dataset(s) referenced in the query.
            location: 'US',
            params: {corpus: 'romeoandjuliet', min_word_count: 250},
            useQueryCache: false,
        };

        // Run the query as a job
        const [job] = await bigqueryClient.createQueryJob(options);
        console.log(`Job ${job.id} started.`);

        // Wait for the query to finish
        const [rows] = await job.getQueryResults();

        // Print the results
        console.log('Rows:');
        rows.forEach(row => console.log(row));

        // Print job statistics
        const creation_time = convertTimestampToDateTime(job.metadata.statistics.creationTime);
        const start_time = convertTimestampToDateTime(job.metadata.statistics.startTime);
        const end_time = convertTimestampToDateTime(job.metadata.statistics.endTime);

        // console.log(job);
        // console.log('job metadata', job.metadata);
        console.log('job metadata', job.metadata.statistics.endTime);

        console.log('\nJOB STATISTICS:')
        console.log(`Status: ${job.metadata.status.state}`);
        console.log(`Creation time: ${creation_time}`);
        console.log(`Start time: ${start_time} : End time: ${end_time}`);
        console.log(`Statement type: ${job.metadata.statistics.query.statementType}`);
    }
    queryShakespeareDisableCache();
}

main();