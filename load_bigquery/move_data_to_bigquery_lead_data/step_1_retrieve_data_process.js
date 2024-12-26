const fs = require('fs');
const mysql = require('mysql2');

const dotenv = require('dotenv');
dotenv.config({ path: "../../.env" }); // add path to read.env file

const { localLeadDbConfig, csvExportPath } = require('../../utilities/config');
const { createLocalDBConnection } = require('../../utilities/connectionLocalDB');

const { leadQuery, leadMetricsQuery } = require('./query_data');

const { getCurrentDateTime, getCurrentDateTimeForFileNaming } = require('../../utilities/getCurrentDate');
const { generateLogFile } = require('../../utilities/generateLogFile');

// STEP #0 - MOVE FILES TO ARCHIVE
function moveFilesToArchive() {
    try {
        // List all files in the directory
        const files = fs.readdirSync(`${csvExportPath}bigquery_leads`);
        console.log(files);

        // Create the "archive" directory if it doesn't exist
        const archivePath = `${csvExportPath}bigquery_leads_archive`;
        fs.mkdirSync((archivePath), { recursive: true });

        // Iterate through each file
        for (const file of files) {
            if (file.endsWith('.csv')) {
                // Construct the full file paths
                const sourceFilePath = `${csvExportPath}bigquery_leads/${file}`;
                const destinationFilePath = `${archivePath}/${file}`;

                // console.log(sourceFilePath);
                // console.log(destinationFilePath);

                try {
                    // Move the file to the "archive" directory
                    fs.renameSync(sourceFilePath, destinationFilePath);
                    // console.log(`Archived ${file}`);
                    generateLogFile('archive_big_query', `Archived ${file}`, csvExportPath);
                } catch (archiveErr) {
                    console.error(`Error moving file ${file} to archive:`, archiveErr);
                    generateLogFile('archive_big_query', `Error archive file ${file}: ${archiveErr}`, csvExportPath);
                }
            }
        }

    } catch (readErr) {
        console.error('Error reading files:', readErr);
    }
}

// STEP #1 - RETRIEVE BOOKING, KEY METRICS, PACING DATA
//todo:
async function execute_get_data(pool, file_name, query) {
    return new Promise((resolve, reject) => {

        const startTime = performance.now();

        pool.query(query, (queryError, results) => {
            const endTime = performance.now();
            const elapsedTime = ((endTime - startTime) / 1_000).toFixed(2); //convert ms to sec

            if (queryError) {
                console.error('Error executing select query:', queryError);
                reject(queryError);
            } else {

                console.log(`GET DATA ${file_name}`);
                // console.table(results); //todo:
                // console.log(Object.keys(results[0]));
                console.log(`Query results: ${results.length}, Elapsed Time: ${elapsedTime} sec`);
                resolve(results);
            }
        });
    });
}

// STEP #1.1a EXPORT RESULTS TO CSV FILE
function export_results_to_csv(results, file_name) {
    if (results.length === 0) {
        console.log('No results to export.');
        generateLogFile('load_big_query', 'No results to export.', csvExportPath);
        return;
    }

    // DEFINE DIRECTORY PATH
    const directoryPath = `${csvExportPath}bigquery_leads`;
    // console.log('Directory path = ', directoryPath);

    // CHECK IF DIRECTORY EXISTS, IF NOT, CREATE IT
    if (!fs.existsSync(directoryPath)) {
        fs.mkdirSync(directoryPath, { recursive: true });
        // console.log(`Directory created: ${directoryPath}`);
    }

    //move files in bigquery_leads directory to bigquery_leads_archive

    try {
        const header = Object.keys(results[0]);

        const csvContent = `${header.join(',')}\n${results.map(row =>
            header.map(key => (row[key] !== null ? row[key] : 'NULL')).join(',')
        ).join('\n')}`;

        const createdAtFormatted = getCurrentDateTimeForFileNaming();
        const filePath = `${csvExportPath}bigquery_leads/results_${createdAtFormatted}_${file_name}.csv`;
        // console.log('File path = ', filePath);

        fs.writeFileSync(filePath, csvContent);

        console.log(`Results exported to ${filePath}`);
        generateLogFile('load_big_query', `Results exported to ${filePath}`, csvExportPath);

    } catch (error) {
        console.error(`Error exporting results to csv:`, error);
        generateLogFile('load_big_query', `Error exporting results to csv: ${error}`, csvExportPath);
    }
}

// MAIN FUNCTION TO EXECUTE THE PROCESS
async function execute_retrieve_data() {
    let pool = "";
    const startTime = performance.now();

    try {
        const getData = [
            {
                poolName: localLeadDbConfig,
                fileName: 'lead_data',
                query: leadQuery
            },
            {
                poolName: localLeadDbConfig,
                fileName: 'lead_metrics_data',
                query: leadMetricsQuery
            },
        ];

        // STEP 1.0 ARCHIVE FILES
        console.log(`\nSTEP 1.0: ARCHIVE FILES`);
        console.log(`${getCurrentDateTime()}\n`);
        moveFilesToArchive();

        // STEP 1.1 PULL SQL DATA FROM BOOKING, KEY METRICS & PACING METRICS TABLES
        console.log(`\nSTEP 1.1: PULL SQL DATA FROM LEAD DATA TABLE`);
        console.log(`${getCurrentDateTime()}\n`);

        for (let i = 0; i < getData.length; i++) {
            const { poolName, fileName, query } = getData[i];

            pool = await createLocalDBConnection(poolName);

            let results = await execute_get_data(pool, fileName, query);
            // console.log(results); //fix

            // STEP 1.1a SAVE DATA TO CSV FILE
            console.log(`STEP 1.1a SAVE ${fileName} TO CSV FILE`);

            export_results_to_csv(results, fileName);

            // CLOSE POOL
            await pool.end(err => {
                if (err) {
                    console.error('Error closing connection pool:', err.message);
                } else {
                    console.log('Connection pool closed successfully.\n');
                }
            });
        }

        console.log('All queries executed successfully.');

    } catch (error) {
        console.error('Error:', error);

    } finally {
        // CLOSE POOL
        await pool.end(err => {
            if (err) {
                console.error('Error closing connection pool:', err.message);
            } else {
                console.log('Connection pool closed successfully.\n');
            }
        });

        const endTime = performance.now();
        const elapsedTime = ((endTime - startTime) / 1_000).toFixed(2); //convert ms to sec

        return elapsedTime;
    }
}

// Run the main function
// execute_retrieve_data();

module.exports = {
    execute_retrieve_data,
}