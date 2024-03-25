const fs = require('fs').promises; // promses necessary for "fs.readdir"
const mysql = require('mysql2');
const { localBookingDbConfig, csvExportPath } = require('../utilities/config');
const { createLocalDBConnection } = require('../utilities/connectionLocalDB');
const { schema_booking_table } = require('./schema_booking_table');
const { createLoadBookingDataQuery } = require('./query_load_data');
const { getCurrentDateTime } = require('../utilities/getCurrentDate');
const { generateLogFile } = require('../utilities/generateLogFile');

// STEP 2.1 DROP "BOOKING DATA" TABLE
async function executeDropTableQuery(pool, table) {
  return new Promise((resolve, reject) => {

    const startTime = performance.now();

    const dropTable = `DROP TABLE IF EXISTS ${table};`;

    pool.query(dropTable, (queryError, results) => {
      const endTime = performance.now();
      const elapsedTime = ((endTime - startTime) / 1_000).toFixed(2); //convert ms to sec

      if (queryError) {
        console.error('Error executing select query:', queryError);
        reject(queryError);
      } else {
        console.log(`\nDrop table results= ${table}`);
        console.table(results);
        console.log(`Query results: ${results.info}, Elapsed Time: ${elapsedTime} sec\n`);
        resolve();
      }
    })
  });
}

// STEP #2.2 - CREATE "BOOKING DATA" TABLE
async function executeCreateBookingDataTable(pool) {
  return new Promise((resolve, reject) => {

    const startTime = performance.now();

    const query = schema_booking_table;

    pool.query(query, (queryError, results) => {
      const endTime = performance.now();
      const elapsedTime = ((endTime - startTime) / 1_000).toFixed(2); //convert ms to sec

      if (queryError) {
        console.error('Error executing select query:', queryError);
        reject(queryError);
      } else {

        console.log('\nTable created successfully.');
        console.table(results);
        console.log(`Query results: ${results.info}, Elapsed Time: ${elapsedTime} sec\n`);
        resolve();
      }
    });
  });
}

// STEP STEP #2.3 - GET FILES IN DIRECTORY / LOAD INTO "BOOKING DATA" TABLE
async function executeInsertBookingDataQuery(pool, filePath, rowsAdded) {

  console.log(filePath);

  return new Promise((resolve, reject) => {

    const startTime = performance.now();

    const query = createLoadBookingDataQuery(filePath);

    pool.query(query, (queryError, results) => {
      const endTime = performance.now();
      const elapsedTime = ((endTime - startTime) / 1_000).toFixed(2); //convert ms to sec
      
      if (queryError) {
        console.error('Error executing select query:', queryError);
        reject(queryError);
      } else {

        console.log(`Data loaded successfully from ${filePath}.`);
        console.log(`Query results: ${results.info}, Elapsed Time: ${elapsedTime} sec`);

        generateLogFile('loading_booking_data', `Rows added: ${results.info}`, csvExportPath);

        rowsAdded = parseInt(results.affectedRows);
        resolve(rowsAdded);
      }
    });
  });
}

// STEP #2.4 - INSERT "CREATED AT" DATE
async function executeInsertCreatedAtQuery(pool, table) {
  return new Promise((resolve, reject) => {

    const startTime = performance.now();

    const addCreateAtDate = `
          ALTER TABLE ${table} ADD COLUMN created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
      `;

    pool.query(addCreateAtDate, (queryError, results) => {
      const endTime = performance.now();
      const elapsedTime = ((endTime - startTime) / 1_000).toFixed(2); //convert ms to sec

      if (queryError) {
        console.error('Error executing select query:', queryError);
        reject(queryError);
      } else {
        console.log('\nInsert "created at" date');
        console.table(results);
        console.log(`Query results: ${results.info}, Elapsed Time: ${elapsedTime} sec\n`);

        resolve();
      }
    });
  });
}

// Main function to handle SSH connection and execute queries
async function execute_load_booking_data() {
  try {
    const pool = await createLocalDBConnection(localBookingDbConfig);
    // console.log(pool.config.connectionConfig.user, pool.config.connectionConfig.database);

    let table = 'booking_data';

    // STEP 2.1: DROP THE "BOOKING DATA" TABLE
    console.log(`STEP 2.1: DROP THE "BOOKING DATA" TABLE`);
    console.log(getCurrentDateTime());
    await executeDropTableQuery(pool, `${table};`);

    // STEP #2.2 - CREATE "BOOKING DATA" TABLE
    console.log(`STEP #2.2 - CREATE "BOOKING DATA" TABLE`);
    console.log(getCurrentDateTime());
    await executeCreateBookingDataTable(pool);

    // STEP #2.3 - GET FILES IN DIRECTORY / LOAD INTO "BOOKING DATA" TABLE
    console.log(`STEP #2.3 - GET FILES IN DIRECTORY / LOAD INTO "BOOKING DATA" TABLE`);
    console.log(getCurrentDateTime());
    let rowsAdded = 0;
    const directory = csvExportPath; // Directory containing your CSV files
    // List all files in the directory
    const files = await fs.readdir(directory);
    let numberOfFiles = 0;

    // Iterate through each file
    for (let i = 0; i < files.length; i++) {
      let currentFile = files[i];
      
      if (currentFile.endsWith('.csv')) {
        numberOfFiles++;

        // Construct the full file path
        const filePath = `${directory}${currentFile}`;

        // Insert file into "booking_data" table
        let query = await executeInsertBookingDataQuery(pool, filePath, rowsAdded);

        // track number of rows added
        rowsAdded += parseInt(query);
        console.log(`File ${i} of ${files.length}`);
        console.log(`Rows added = ${rowsAdded}\n`);
      }
    }

    generateLogFile('loading_booking_data', `Total files added = ${numberOfFiles} Total rows added = ${rowsAdded.toLocaleString()}`, csvExportPath);
    console.log('Files processed = ', numberOfFiles);

    // STEP #2.4 - INSERT "CREATED AT" DATE
    await executeInsertCreatedAtQuery(pool, `${table}`);

    console.log('All queries executed successfully.');

    await pool.end(err => {
      if (err) {
        console.error('Error closing connection pool:', err.message);
      } else {
        console.log('Connection pool closed successfully.');
      }
    });

  } catch (error) {
    console.error('Catch error:', error);
    generateLogFile('loading_booking_data', `Error loading booking data: ${error}`, csvExportPath);
  } finally {
    // TODO
  }
}

// Call the function
// execute_load_booking_data();

module.exports = {
  execute_load_booking_data,
}
