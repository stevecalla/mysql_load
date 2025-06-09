'use strict';

// Import the Google Cloud client library
const { BigQuery } = require('@google-cloud/bigquery');
const BQ_CREDENTIALS_PATH = require('../auth_certs/cool-ship-418513-cadf086380e7_key2.json');

function main() {
    async function queryShakespeare() {
        // Create a client with custom credentials
        const bigqueryClient = new BigQuery({
            credentials: BQ_CREDENTIALS_PATH,
        });

        // The SQL query to run
        const sqlQuery = `SELECT word, word_count
            FROM \`bigquery-public-data.samples.shakespeare\`
            WHERE corpus = @corpus
            AND word_count >= @min_word_count
            ORDER BY word_count DESC`;

        const options = {
            query: sqlQuery,
            // Location must match that of the dataset(s) referenced in the query.
            location: 'US',
            params: { corpus: 'romeoandjuliet', min_word_count: 250 },
        };

        // Run the query
        const [rows] = await bigqueryClient.query(options);

        console.log('Rows:');
        rows.forEach(row => console.log(row));
    }

    queryShakespeare();
}

main();
