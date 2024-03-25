const fs = require('fs');
const mysql = require('mysql2');
const { Client } = require('ssh2');
const sshClient = new Client();
const { forwardConfig , dbConfig, sshConfig, csvExportPath } = require('../utilities/config');
const { queryBookingData } = require('./query_BookingData');
const { generateLogFile } = require('../utilities/generateLogFile');
const { getCurrentDateTimeForFileNaming} = require('../utilities/getCurrentDate');

// console.log('process env', process.env);
// console.log('sshConfig', sshConfig);

// Function to create a Promise for managing the SSH connection and MySQL queries
function createSSHConnection() {
    return new Promise((resolve, reject) => {
        sshClient.on('ready', () => {
            console.log('SSH tunnel established.');

            const { srcHost, srcPort, dstHost, dstPort } = forwardConfig;
            sshClient.forwardOut(
                srcHost,
                srcPort,
                dstHost,
                dstPort,
                (err, stream) => {
                    if (err) reject(err);

                    const updatedDbServer = {
                        ...dbConfig,
                        stream,
                        ssl: {
                            rejectUnauthorized: false,
                        },
                    };

                    const pool = mysql.createPool(updatedDbServer);

                    resolve(pool);
                }
            );
        }).connect(sshConfig);
    });
}

// Function to execute query for a single date range
async function executeQueryForDateRange(pool, startDate, endDate) {
    return new Promise((resolve, reject) => {

        const startTime = performance.now();

        const modifiedBookingQuery = queryBookingData
            .replace('startDateVariable', startDate)
            .replace('endDateVariable', endDate);

        pool.query(modifiedBookingQuery, (queryError, results) => {
            const endTime = performance.now();
            const elapsedTime = ((endTime - startTime) / 1_000).toFixed(2); //convert ms to sec

            if (queryError) {
                console.error('Error executing select query:', queryError);
                reject(queryError);
            } else {

                // results.forEach(result => console.log(result));

                console.log(`Query results length: ${results.length}, Elapsed Time: ${elapsedTime} sec`);

                generateLogFile('booking_data', `Query results length: ${results.length}, Elapsed Time: ${elapsedTime} sec`, csvExportPath);
                exportResultsToCSV(results, startDate, endDate);
                resolve();
            }
        });
    });
}

// Function to export results to a CSV file
function exportResultsToCSV(results, startDate, endDate) {
    if (results.length === 0) {
        console.log('No results to export.');
        generateLogFile('booking_data', 'No results to export.', csvExportPath);
        return;
    }

    try {
        const header = Object.keys(results[0]);
        const csvContent = `${header.join(',')}\n${results.map(row =>
            header.map(key => (row[key] !== null ? row[key] : 'NULL')).join(',')
        ).join('\n')}`;

        const createdAtFormatted = getCurrentDateTimeForFileNaming();

        const saveFilePath = `${csvExportPath}results_${createdAtFormatted}_${startDate}_${endDate}.csv`;
        console.log(saveFilePath);

        fs.writeFileSync(saveFilePath, csvContent);

        console.log(`Results exported to ${csvExportPath}\n`);
        generateLogFile('booking_data', `Results exported to ${saveFilePath}`, csvExportPath);

    } catch (error) {
        console.error(`Error exporting results to csv:`, error);
        generateLogFile('booking_data', `Error exporting results to csv: ${error}`, csvExportPath);
    }
}

function moveFilesToArchive() {
    console.log('Moving files to archive');

    try {
        // List all files in the directory
        const files = fs.readdirSync(csvExportPath);

        // Create the "archive" directory if it doesn't exist
        const archivePath = `${csvExportPath}archive`;
        fs.mkdirSync((archivePath), { recursive: true });

        // Iterate through each file
        for (const file of files) {
            if (file.endsWith('.csv')) {
                // Construct the full file paths
                const sourceFilePath = `${csvExportPath}${file}`;
                const destinationFilePath = `${archivePath}/${file}`;

                try {
                    // Move the file to the "archive" directory
                    fs.renameSync(sourceFilePath, destinationFilePath);
                    console.log(`Archived ${file}`);
                    generateLogFile('booking_data', `Archived ${file}`, csvExportPath);
                } catch (archiveErr) {
                    console.error(`Error moving file ${file} to archive:`, archiveErr);
                    generateLogFile('booking_data', `Error archive file ${file}: ${archiveErr}`, csvExportPath);
                }
            }
        }

    } catch (readErr) {
        console.error('Error reading files:', readErr);
    }
}

function deleteArchivedFiles() {
    console.log('Deleting files from archive');
    // List all files in the directory
    const files = fs.readdirSync(`${csvExportPath}/archive`);

    console.log(files);

    // Iterate through each file
    files?.forEach((file) => {
        if (file.endsWith('.csv')) {
            // Construct the full file path
            const filePath = `${csvExportPath}archive/${file}`;
            console.log(filePath);

            try {
                // Delete the file
                fs.unlinkSync(filePath);
                console.log(`File ${filePath} deleted successfully.`);
                generateLogFile('booking_data', `File ${filePath} deleted successfully.`, csvExportPath);
            } catch (deleteErr) {
                console.error(`Error deleting file ${filePath}:`, deleteErr);
                generateLogFile('booking_data', `Error deleting file ${filePath}: ${deleteErr}`, csvExportPath);
            }
        }
    });
}

// Main function to handle SSH connection and execute queries
async function execute_get_booking_data() {
    try {
        const startTime = performance.now();

        deleteArchivedFiles();
        moveFilesToArchive();

        const pool = await createSSHConnection();

        // Array of date ranges to loop through
        const dateRanges = [
            { startDate: '2024-01-01', endDate: '2024-12-31' }, // OKAY

            // // 2023
            { startDate: '2023-10-01', endDate: '2023-12-31' }, // OKAY
            { startDate: '2023-07-01', endDate: '2023-09-30' }, // OKAY
            { startDate: '2023-04-01', endDate: '2023-06-30' }, // OKAY
            { startDate: '2023-01-01', endDate: '2023-03-31' }, // OKAY

            // // // 2022
            { startDate: '2022-10-01', endDate: '2022-12-31' }, // OKAY
            { startDate: '2022-07-01', endDate: '2022-09-30' }, // OKAY
            { startDate: '2022-04-01', endDate: '2022-06-30' }, // OKAY
            { startDate: '2022-01-01', endDate: '2022-03-31' }, // OKAY

            // // // 2021
            { startDate: '2021-07-01', endDate: '2021-12-31' }, // OKAY
            { startDate: '2021-01-01', endDate: '2021-06-30' }, // OKAY

            // // // 2017 - 2020
            { startDate: '2020-01-01', endDate: '2020-12-31' }, // 
            { startDate: '2019-01-01', endDate: '2019-12-31' }, // 
            { startDate: '2018-01-01', endDate: '2018-12-31' }, // 
            { startDate: '2017-01-01', endDate: '2017-12-31' }, // 
            { startDate: '2015-01-01', endDate: '2016-12-31' }, // 
            // Add more date ranges as needed
        ];

        // Execute queries for each date range
        for (const { startDate, endDate } of dateRanges) {
            console.log(`Started query for ${startDate} to ${endDate}.`);

            await executeQueryForDateRange(pool, startDate, endDate);
            
            generateLogFile('booking_data', `Query for ${startDate} to ${endDate} executed successfully.`, csvExportPath);
        }

        // Close the SSH connection after all queries are executed
        // sshClient.end(); //revised below to include error messaging
        sshClient.end(err => {
            if (err) {
              console.error('Error closing SSH connection pool:', err.message);
            } else {
              console.log('SSH Connection pool closed successfully.');
            }
          });
  
          await pool.end(err => {
            if (err) {
              console.error('Error closing connection pool:', err.message);
            } else {
              console.log('Connection pool closed successfully.');
            }
          });

          const endTime = performance.now();
          const elapsedTime = ((endTime - startTime) / 1_000).toFixed(2); //convert ms to sec
          // MOVED THE MESSAGE BELOW TO THE BOOKING_JOB_032024 PROCESS
        //   console.log(`\nAll get booking data queries executed successfully. Elapsed Time: ${elapsedTime ? elapsedTime : "Opps error getting time"} sec\n`);
          return elapsedTime;

    } catch (error) {
        console.error('Error:', error);

    }
}

// Run the main function
// execute_get_booking_data();

module.exports = {
    execute_get_booking_data,
}