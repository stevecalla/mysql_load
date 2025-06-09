const fs = require('fs');
const path = require('path');
const fastcsv = require('fast-csv');
// const mysql = require('mysql2');

const dotenv = require('dotenv');
dotenv.config({ path: "../../.env" }); // add path to read.env file

const { localLeadDbConfig, csvExportPath } = require('../../utilities/config');
const { createLocalDBConnection } = require('../../utilities/connectionLocalDB');

const { determineOSPath } = require('../../utilities/determineOSPath');
const { create_directory } = require('../../utilities/createDirectory');

const { leadQuery, leadMetricsQuery } = require('./query_data');

const { getCurrentDateTime, getCurrentDateTimeForFileNaming } = require('../../utilities/getCurrentDate');
const { generateLogFile } = require('../../utilities/generateLogFile');
const { runTimer, stopTimer } = require('../../utilities/timer');

// STEP #1 - DELETE ARCHIVED FILES
async function deleteArchivedFiles() {
    console.log('Deleting files from archive');

    // Create the "archive" directory if it doesn't exist
    const directoryName  = `bigquery_leads_archive`;
    const directoryPath = await create_directory(directoryName);

    // List all files in the directory
    const files = fs.readdirSync(directoryPath);
    console.log(files);

    const logPath = await determineOSPath();

    // Iterate through each file
    files?.forEach((file) => {
        if (file.endsWith('.csv')) {
            // Construct the full file path
            const filePath = `${directoryPath}/${file}`;
            console.log(filePath);

            try {
                // Delete the file
                fs.unlinkSync(filePath);
                console.log(`File ${filePath} deleted successfully.`);
                generateLogFile('get_usat_sales_data', `File ${filePath} deleted successfully.`, logPath);
            } catch (deleteErr) {
                console.error(`Error deleting file ${filePath}:`, deleteErr);
                generateLogFile('get_usat_sales_data', `Error deleting file ${filePath}: ${deleteErr}`, logPath);
            }
        }
    });
}

// STEP #2 - MOVE FILES TO ARCHIVE
async function moveFilesToArchive() {
    console.log('Moving files to archive');

    const os_path = await determineOSPath();

    try {
        // List all files in the directory
        let directoryName = `bigquery_leads`;
        const sourcePath = `${os_path}${directoryName}`;
        // await create_directory('bigquery_leads');
        
        const files = fs.readdirSync(sourcePath);
        console.log(files);

        // Create the "archive" directory if it doesn't exist
        directoryName  = `bigquery_leads_archive`;
        const destinationPath = await create_directory(directoryName);
        console.log(destinationPath);

        // Iterate through each file
        for (const file of files) {
            if (file.endsWith('.csv')) {
                // Construct the full file paths
                const sourceFilePath = `${sourcePath}/${file}`;
                const destinationFilePath = `${destinationPath}/${file}`;

                try {
                    // Move the file to the "archive" directory
                    fs.renameSync(sourceFilePath, destinationFilePath);
                    console.log(`Archived ${file}`);
                    // generateLogFile('get_user_data', `Archived ${file}`, logPath);
                } catch (archiveErr) {
                    console.error(`Error moving file ${file} to archive:`, archiveErr);
                    generateLogFile('get_user_data', `Error archive file ${file}: ${archiveErr}`, logPath);
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
async function export_results_to_csv_fast_csv(results, file_name, i) {
    console.log('STEP #4 EXPORT RESULTS TO CSV FILE', `${i}_export file_name`);
    const startTime = performance.now(); // Start timing
    const logPath = await determineOSPath();

    if (results.length === 0) {
        console.log('No results to export.');
        generateLogFile('get_usat_sales_data', 'No results to export.', logPath);
        return;
    }

    // DEFINE DIRECTORY PATH
    const directoryName  = `bigquery_leads`;
    const directoryPath = await create_directory(directoryName);

    console.log('Directory path = ', directoryPath);

    try {
        const header = Object.keys(results[0]);

        // Create file path with timestamp
        const created_at_formatted = getCurrentDateTimeForFileNaming();
        const filePath = path.join(directoryPath, `results_${created_at_formatted}_${file_name}.csv`);
        // console.log('File path = ', filePath);

        // Create a writable stream to the file
        const writeStream = fs.createWriteStream(filePath);
        
        // Create a fast-csv stream
        const csvStream = fastcsv.format({ headers: true });
        
        // Pipe the csv stream to the writable stream
        csvStream.pipe(writeStream);

        // Write the rows
        results.forEach(row => {
            csvStream.write(header.reduce((acc, key) => ({
                ...acc,
                [key]: row[key] !== null ? row[key] : 'NULL'
            }), {}));
        });

        // End the CSV stream
        csvStream.end();

        // Await stream finish
        await new Promise((resolve, reject) => {
            writeStream.on('finish', resolve);
            writeStream.on('error', reject);
        });

        // Calculate elapsed time
        const endTime = performance.now();
        const elapsedTime = ((endTime - startTime) / 1_000).toFixed(2); // Convert ms to sec

        console.log(`STEP #4 EXPORT RESULTS TO CSV FILE: Elapsed Time: ${elapsedTime} sec`);

        console.log(`Results exported to ${filePath}`);
        generateLogFile('get_usat_sales_data', `User data exported to ${filePath}`, logPath);

        return;

    } catch (error) {
        console.error(`Error exporting results to csv:`, error);
        generateLogFile('get_usat_sales_data', `Error exporting results to csv: ${error}`, logPath);
    } finally {
    }
}

// MAIN FUNCTION TO EXECUTE THE PROCESS
async function execute_retrieve_data() {
    let pool = "";
    const startTime = performance.now();

    try {
        // STEP #1: DELETE PRIOR FILES
        await deleteArchivedFiles();

        // STEP #2 - MOVE FILES TO ARCHIVE
        await moveFilesToArchive();

        // STEP 3 PULL SQL DATA FROM BOOKING, KEY METRICS & PACING METRICS TABLES
        console.log(`\nSTEP 3: PULL SQL DATA FROM LEAD DATA TABLE`);
        console.log(`${getCurrentDateTime()}\n`);

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

        for (let i = 0; i < getData.length; i++) {
            runTimer(`0_get_data`);

            const { poolName, fileName, query } = getData[i];

            pool = await createLocalDBConnection(poolName);

            let results = await execute_get_data(pool, fileName, query);
            // console.log(results);

            // STEP 3a SAVE DATA TO CSV FILE
            console.log(`STEP 3a SAVE ${fileName} TO CSV FILE`);

            // export_results_to_csv(results, fileName); 
            await export_results_to_csv_fast_csv(results, fileName, i); 

            stopTimer(`0_get_data`);
            
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
        stopTimer(`0_get_data`);

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