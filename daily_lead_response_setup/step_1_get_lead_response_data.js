const fs = require('fs');
const path = require('path');
const dotenv = require('dotenv');
dotenv.config();

const mysql = require('mysql2');
const { Client } = require('ssh2');
const sshClient = new Client();
const { forwardConfig, dbConfigLeadsProduction, sshConfigLeadsProduction  } = require('../utilities/config');

const { determineOSPath } = require('../utilities/determineOSPath');
const { create_directory } = require('../utilities/createDirectory');

const fastcsv = require('fast-csv');

const { query_multiple_leads_per_booking } = require('./queries/get_lead_response_data/query_multiple_leads_per_booking_120624');
const { query_single_lead_per_booking } = require('./queries/get_lead_response_data/query_single_lead_per_booking_120624');
const { query_leads_no_booking } = require('./queries/get_lead_response_data/query_leads_no_booking_120624');

const { generateLogFile } = require('../utilities/generateLogFile');
const { getCurrentDateTimeForFileNaming, getPakistanTime } = require('../utilities/getCurrentDate');
const { runTimer, stopTimer } = require('../utilities/timer');

// Function to create a Promise for managing the SSH connection and MySQL queries
function createSSHConnection() {

    const { srcHost, srcPort, dstHost, dstPort } = forwardConfig;
    const db = dbConfigLeadsProduction;
    const ssh = sshConfigLeadsProduction;

    // console.log('db = ', db);
    // console.log('ssh = ', ssh);
    // console.log('forward config = ', forwardConfig);
    
    return new Promise((resolve, reject) => {
        sshClient.on('ready', () => {
            console.log('\nSSH tunnel established.\n');

            sshClient.forwardOut(
                srcHost,
                srcPort,
                dstHost,
                dstPort,
                (err, stream) => {
                    if (err) reject(err);

                    const updatedDbServer = {
                        ...db,  
                        stream,
                        ssl: {
                            rejectUnauthorized: false,
                        },
                    };

                    const pool = mysql.createPool(updatedDbServer);

                    resolve(pool);
                }
            );
        }).connect(ssh);
    });
}

// STEP #1 - DELETE ARCHIVED FILES
async function deleteArchivedFiles() {
    console.log('Deleting files from archive');

    // Create the "archive" directory if it doesn't exist
    const directoryName  = `ezhire_lead_data_archive`;
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
        let directoryName = `ezhire_lead_data`;
        const sourcePath = `${os_path}${directoryName}`;
        await create_directory('ezhire_lead_data');
        const files = fs.readdirSync(sourcePath);
        console.log(files);

        // Create the "archive" directory if it doesn't exist
        directoryName  = `ezhire_lead_data_archive`;
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

// STEP #3: GET / QUERY USER DATA & RETURN RESULTS
async function execute_query_get_usat_sales_data(pool, query) {
    const startTime = performance.now(); // Start timing
    const logPath = await determineOSPath();

    // console.log(query);

    try {
        const results = await new Promise((resolve, reject) => {
            pool.query(query, (queryError, results) => {
                if (queryError) {
                    reject(queryError);
                } else {
                    resolve(results);
                }
            });
        });

        // Calculate elapsed time
        const endTime = performance.now();
        const elapsedTime = ((endTime - startTime) / 1_000).toFixed(2); // Convert ms to sec

        // Log results and elapsed time
        // console.log(`\n\nQuery results: `);
        // console.table(results);
        console.log(`\nQuery results length: ${results.length}, Elapsed Time: ${elapsedTime} sec`);

        // Additional operations (optional)
        generateLogFile('get_usat_sales_data', `Query results length: ${results.length}, Elapsed Time: ${elapsedTime} sec`, logPath);

        return results; // Return results if needed

    } catch (error) {
        // Handle errors
        console.error('Error executing select query:', error);
        throw error; // Rethrow error if needed
    } finally {
    }
}

// STEP #4 EXPORT RESULTS TO CSV FILE
async function export_results_to_csv(results, file_name, i) {
    console.log('STEP #4 EXPORT RESULTS TO CSV FILE', `${i}_export ${file_name}`);
    // const startTime = performance.now(); // Start timing
    const logPath = await determineOSPath();

    if (results.length === 0) {
        console.log('No results to export.');
        generateLogFile('get_usat_sales_data', 'No results to export.', logPath);
        return;
    }

    // DEFINE DIRECTORY PATH
    const directoryName  = `ezhire_lead_data`;
    const directoryPath = await create_directory(directoryName);

    try {
        const header = Object.keys(results[0]);

        const csvContent = `${header.join(',')}\n${results.map(row =>
            header.map(key => (row[key] !== null ? row[key] : 'NULL')).join(',')
        ).join('\n')}`;

        const created_at_formatted = getCurrentDateTimeForFileNaming();
        const filePath = path.join(directoryPath, `results_${created_at_formatted}_${file_name}.csv`);
        // console.log('File path = ', filePath);

        fs.writeFileSync(filePath, csvContent);

        console.log(`Results exported to ${filePath}`);
        generateLogFile('load_big_query', `Results exported to ${filePath}`, logPath);

    } catch (error) {
        console.error(`Error exporting results to csv:`, error);

        generateLogFile('load_big_query', `Error exporting results to csv: ${error}`, logPath);

        console.log("----------------------------------------------");
        console.log("EXPORT FAILED: RUNNING BACKUP STREAMING EXPORT");
        console.log("----------------------------------------------");

        export_results_to_csv_fast_csv(results, file_name, i);
    }
}

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
    const directoryName  = `ezhire_lead_data`;
    const directoryPath = await create_directory(directoryName);

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

// Main function to handle SSH connection and execute queries
async function execute_get_lead_response_data(date) {
    let pool;
    const startTime = performance.now();
    const logPath = await determineOSPath();

    try {
        // STEP #0: ENSURE FILE WAS UPDATED RECENTLY

        // STEP #1: DELETE PRIOR FILES
        await deleteArchivedFiles(); //todo:

        // STEP #2 - MOVE FILES TO ARCHIVE
        await moveFilesToArchive(); //todo:

        // // STEP #3: GET / QUERY USER DATA & RETURN RESULTS
        pool = await createSSHConnection();

        const query_logic = [
            {
                query: query_multiple_leads_per_booking,
                file_name: 'multiple_leads_per_booking',
            },
            {
                query: query_single_lead_per_booking,
                file_name: 'single_lead_per_booking',
            },
            {
                query: query_leads_no_booking,
                file_name: 'leads_no_booking',
            },
        ];

        for (let i = 0; i < query_logic.length; i++) {
            runTimer(`0_get_data`);

            let { query, file_name } = query_logic[i];
            
            if (!date) date = await getPakistanTime();

            query = await query(date);

            results = await execute_query_get_usat_sales_data(pool, query, date);

            stopTimer(`0_get_data`);

            console.log(`File ${i + 1} of ${query_logic.length} complete.\n`);  

            // generateLogFile('get_usat_sales_data', `Query for  execute_query_get_sales_data executed successfully.`, logPath);  
            
            // STEP #4: EXPORT RESULTS TO CSV
            runTimer(`0_export`);

            let file_name_date = `${file_name}_${date}`

            // added to catch block in export_results_to_csv
            // await export_results_to_csv(results, file_name_date, i); 
            await export_results_to_csv_fast_csv(results, file_name_date, i); 
            
            console.log(file_name_date, date);

            stopTimer(`0_export`); 
        }
    } catch (error) {
        console.error('Error:', error);
        generateLogFile('get_usat_sales_data', `Error loading user data: ${error}`, logPath);

        stopTimer(`0_get_data`);
        stopTimer(`0_export`); 
        
    } finally {
        // Ensure cleanup happens even if there is an error
        if (pool) {
            await new Promise((resolve, reject) => {
                pool.end(err => {
                    if (err) {
                        console.error('Error closing connection pool:', err.message);
                        reject(err);
                    } else {
                        console.log('Connection pool closed successfully.');
                        resolve();
                    }
                });
            });
        }

        if (sshClient) {
            sshClient.end(err => {
                if (err) {
                    console.error('Error closing SSH connection pool:', err.message);
                } else {
                    console.log('SSH Connection pool closed successfully.');
                }
            });
        }

        // LOG RESULTS
        const endTime = performance.now();
        const elapsedTime = ((endTime - startTime) / 1_000).toFixed(2); //convert ms to sec

        console.log(`\nAll get usat sales data queries executed successfully. Elapsed Time: ${elapsedTime ? elapsedTime : "Opps error getting time"} sec\n`);

        // process.exit();

        // return elapsedTime;
    }
}

// Run the main function
execute_get_lead_response_data('2024-12-05');

module.exports = {
    execute_get_lead_response_data,
}