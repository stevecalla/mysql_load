const fs = require('fs');
const mysql = require('mysql2');
const dotenv = require('dotenv');
dotenv.config({ path: "../../.env" }); // adding the path ensures each folder will read the .env file as necessary

const { Client } = require('ssh2');
const sshClient = new Client();
const { forwardConfig , dbConfig, sshConfig, csvExportPath } = require('../../utilities/config');

const { queryUserData } = require('./query_get_all_user_data_excludng_test');
const { generateLogFile } = require('../../utilities/generateLogFile');
const { getCurrentDateTimeForFileNaming } = require('../../utilities/getCurrentDate');
const { query_most_recent_create_on_date } = require('./query_most_recent_created_on');

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

// STEP #1 - DELETE ARCHIVED FILES
async function deleteArchivedFiles() {
    console.log('Deleting files from archive');
    
    // List all files in the directory
    const files = fs.readdirSync(`${csvExportPath}user_data_archive`);
    console.log(files);

    // Iterate through each file
    files?.forEach((file) => {
        if (file.endsWith('.csv')) {
            // Construct the full file path
            const filePath = `${csvExportPath}user_data_archive/${file}`;
            console.log(filePath);

            try {
                // Delete the file
                fs.unlinkSync(filePath);
                console.log(`File ${filePath} deleted successfully.`);
                generateLogFile('get_user_data', `File ${filePath} deleted successfully.`, csvExportPath);
            } catch (deleteErr) {
                console.error(`Error deleting file ${filePath}:`, deleteErr);
                generateLogFile('get_user_data', `Error deleting file ${filePath}: ${deleteErr}`, csvExportPath);
            }
        }
    });
}

// STEP #2 - MOVE FILES TO ARCHIVE
async function moveFilesToArchive() {
    console.log('Moving files to archive');

    try {
        // List all files in the directory
        const files = fs.readdirSync(`${csvExportPath}user_data`);
        console.log(files);

        // Create the "archive" directory if it doesn't exist
        const archivePath = `${csvExportPath}user_data_archive`;
        fs.mkdirSync((archivePath), { recursive: true });

        // Iterate through each file
        for (const file of files) {
            if (file.endsWith('.csv')) {
                // Construct the full file paths
                const sourceFilePath = `${csvExportPath}user_data/${file}`;
                const destinationFilePath = `${archivePath}/${file}`;

                try {
                    // Move the file to the "archive" directory
                    fs.renameSync(sourceFilePath, destinationFilePath);
                    console.log(`Archived ${file}`);
                    generateLogFile('get_user_data', `Archived ${file}`, csvExportPath);
                } catch (archiveErr) {
                    console.error(`Error moving file ${file} to archive:`, archiveErr);
                    generateLogFile('get_user_data', `Error archive file ${file}: ${archiveErr}`, csvExportPath);
                }
            }
        }

    } catch (readErr) {
        console.error('Error reading files:', readErr);
    }
}

// STEP #3: GET / QUERY USER DATA & RETURN RESULTS
async function execute_query_get_user_data(pool) {
    return new Promise((resolve, reject) => {

        const startTime = performance.now();

        const query = queryUserData;

        pool.query(query, (queryError, results) => {
            const endTime = performance.now();
            const elapsedTime = ((endTime - startTime) / 1_000).toFixed(2); //convert ms to sec

            if (queryError) {
                console.error('Error executing select query:', queryError);
                reject(queryError);
            } else {

                // results.forEach(result => console.log(result));
                // console.table(results);

                console.log(`Query results length: ${results.length}, Elapsed Time: ${elapsedTime} sec`);

                generateLogFile('get_user_data', `Query results length: ${results.length}, Elapsed Time: ${elapsedTime} sec`, csvExportPath);
                resolve(results);
            }
        });
    });
}

// STEP #4 EXPORT RESULTS TO CSV FILE
async function export_results_to_csv(results) {
    if (results.length === 0) {
        console.log('No results to export.');
        generateLogFile('user_data', 'No results to export.', csvExportPath);
        return;
    }

    // DEFINE DIRECTORY PATH
    const directoryPath = `${csvExportPath}user_data`;
    // console.log('Directory path = ', directoryPath);

    // CHECK IF DIRECTORY EXISTS, IF NOT, CREATE IT
    if (!fs.existsSync(directoryPath)) {
        fs.mkdirSync(directoryPath, { recursive: true });
        // console.log(`Directory created: ${directoryPath}`);
    }

    try {
        const header = Object.keys(results[0]);

        const csvContent = `${header.join(',')}\n${results.map(row =>
            header.map(key => (row[key] !== null ? row[key] : 'NULL')).join(',')
        ).join('\n')}`;

        const createdAtFormatted = getCurrentDateTimeForFileNaming();
        const filePath = `${directoryPath}/results_${createdAtFormatted}_user_data.csv`;
        // console.log('File path = ', filePath);

        fs.writeFileSync(filePath, csvContent);

        console.log(`Results exported to ${csvExportPath}`);
        generateLogFile('user_data', `User data exported to ${filePath}`, csvExportPath);

    } catch (error) {
        console.error(`Error exporting results to csv:`, error);
        generateLogFile('get_user_data', `Error exporting results to csv: ${error}`, csvExportPath);
    }
}

// Main function to handle SSH connection and execute queries
async function execute_get_user_data() {
    try {
        const startTime = performance.now();

        // STEP #0: ENSURE FILE WAS UPDATED RECENTLY

        // STEP #1: DELETE PRIOR FILES
        await deleteArchivedFiles();

        // STEP #2 - MOVE FILES TO ARCHIVE
        await moveFilesToArchive();

        // STEP #3: GET / QUERY USER DATA & RETURN RESULTS
        const pool = await createSSHConnection();
        const results = await execute_query_get_user_data(pool);

        // console.log(`File ${i + 1} of ${dateRangesLength} complete.\n`);      
        // generateLogFile('get_user_data', `Query for ${startDate} to ${endDate} executed successfully.`, csvExportPath);  
        
        // STEP #4: EXPORT RESULTS TO CSV
        await export_results_to_csv(results);

        // STEP #5: LOAD DATA INTO MYSQL

        // Close the SSH connection after all queries are executed
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
          console.log(`\nAll get booking data queries executed successfully. Elapsed Time: ${elapsedTime ? elapsedTime : "Opps error getting time"} sec\n`);

          return elapsedTime;

    } catch (error) {
        console.error('Error:', error);

    }
}

// Run the main function
// execute_get_user_data();

module.exports = {
    execute_get_user_data,
}