const fs = require('fs').promises; // promses necessary for "fs.readdir"
const mysql = require('mysql2');
const dotenv = require('dotenv');
dotenv.config({ path: "../../.env" }); // adding the path ensures each folder will read the .env file as necessary

const { localUserDbConfig, csvExportPath } = require('../../utilities/config');
const { createLocalDBConnection } = require('../../utilities/connectionLocalDB');

const { schema_user_data_base_table } = require('./schema_user_data_base_table');
const { load_user_data_query } = require('./query_load_user_data');

const { getCurrentDateTime } = require('../../utilities/getCurrentDate');
const { generateLogFile } = require('../../utilities/generateLogFile');

// STEP #0 - SHOW USER PERMISSION
async function execute_show_permissions(pool) {
  return new Promise((resolve, reject) => {

    const startTime = performance.now();
    
    const query = `SHOW GRANTS FOR 'root'@'localhost';`;

    pool.query(query, (queryError, results) => {
      const endTime = performance.now();
      const elapsedTime = ((endTime - startTime) / 1_000).toFixed(2); // convert ms to sec

      if (queryError) {
        console.error('Error executing SHOW PERMISSIONS query:', queryError);
        reject(queryError);
      } else {
        console.log('\nQuery SHOW PERMISSIONS successfully.');
        console.log(results);
        console.log(`Elapsed Time: ${elapsedTime} sec\n`);
        resolve();
      }
    });
  });
}

// STEP #1 - CREATE "ezhire_user_data" DB
// CAN'T SEEM TO CREATE THE ezhire_user_data' DB; I BELIEVE THE ENV/CONFIG IS LOOKING FOR THE TABLE
async function execute_create_user_database(pool) {
  return new Promise((resolve, reject) => {

    const startTime = performance.now();
    
    // const checkQuery = `SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = 'ezhire_user_data';`;
    const checkQuery = `SHOW GRANTS FOR 'root'@'localhost';`;

    pool.query(checkQuery, (checkError, checkResults) => {
      if (checkError) {
        console.error('Error checking database existence:', checkError);
        reject(checkError);
      } else if (checkResults.length > 0) {
        console.log('Database "ezhire_user_data" already exists.');
        resolve();
      } else {
        const createQuery = `CREATE DATABASE ezhire_user_data;`;

        pool.query(createQuery, (createQueryError, createResults) => {
          const endTime = performance.now();
          const elapsedTime = ((endTime - startTime) / 1_000).toFixed(2); // convert ms to sec

          if (createQueryError) {
            console.error('Error executing CREATE DATABASE query:', createQueryError);
            reject(createQueryError);
          } else {
            console.log('\n"ezhire_user_data" DB created successfully.');
            console.log(`Query results: ${createResults.info}, Elapsed Time: ${elapsedTime} sec\n`);
            resolve();
          }
        });
      }
    });
  });
};

// STEP 2 DROP "USER DATA" TABLE
async function execute_drop_table_query(pool, table) {
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

// STEP #3 - CREATE "user_data_base" TABLE
async function execute_create_user_data_table(pool) {
  return new Promise((resolve, reject) => {

    const startTime = performance.now();

    const query = schema_user_data_base_table;

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

// STEP STEP #4 - GET FILES IN DIRECTORY / LOAD INTO "user_data_base" TABLE
async function read_data(filePath) {
  console.log('read data');
  console.log(filePath);

  try {
    const fileData = await fs.readFile(filePath, 'utf8');
    console.log('Data at file path:', fileData);


    return fileData;
  } catch (error) {
    console.error('Error reading file:', error);
    throw error;
  }
}

async function execute_insert_user_data_query(pool, filePath, rowsAdded) {

  // console.log(filePath);        

  return new Promise((resolve, reject) => {

    const startTime = performance.now();

    const query = load_user_data_query(filePath);

    // console.log(filePath);
    // console.log(query);

    pool.query(query, (queryError, results) => {
      const endTime = performance.now();
      const elapsedTime = ((endTime - startTime) / 1_000).toFixed(2); //convert ms to sec
      
      if (queryError) {
        console.error('Error executing select query:', queryError);
        reject(queryError);
      } else {

        console.log(`Data loaded successfully from ${filePath}.`);
        console.log(`Query results: ${results.info}, Elapsed Time: ${elapsedTime} sec`);

        generateLogFile('loading_user_data', `Rows added: ${results.info}`, csvExportPath);

        rowsAdded = parseInt(results.affectedRows);
        resolve(rowsAdded);
      }
    });
  });
}

// STEP #5 - INSERT "CREATED AT" DATE
async function execute_insert_createdAt_query(pool, table) {
  return new Promise((resolve, reject) => {

    const startTime = performance.now();

    const addCreateAtDate = `ALTER TABLE ${table} ADD COLUMN created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;`;

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
async function execute_load_user_data() {
  let pool;
  const startTime = performance.now();

  try {
    pool = await createLocalDBConnection(localUserDbConfig);
    // console.log(pool.config.connectionConfig.user, pool.config.connectionConfig.database);

    // STEP 0: SHOW PERMISSIONS
    console.log(`\nSTEP 0: SHOW PERMISSIONS`);
    console.log(getCurrentDateTime());
    // await execute_show_permissions(pool);

    // STEP 1: CREATE THE "ezhire_user_data" DB IF DOESN'T EXIST
    console.log(`\nSTEP 1: CREATE THE "ezhire_user_data" DB IF DOESN'T EXIST`);
    console.log(getCurrentDateTime());
    await execute_create_user_database(pool);
    
    // STEP 2: DROP THE "user_data_base" TABLE
    let table = 'user_data_base';
    console.log(`\nSTEP 2: DROP THE "user_data_base" TABLE`);
    console.log(getCurrentDateTime());
    await execute_drop_table_query(pool, `${table};`);

    // STEP #3 - CREATE "USER DATA" TABLE
    console.log(`STEP #3 - CREATE "user_data_base" TABLE`);
    console.log(getCurrentDateTime());
    await execute_create_user_data_table(pool);

    // STEP #4 - GET FILES IN DIRECTORY / LOAD INTO "USER DATA" TABLE
    console.log(`STEP #4 - GET FILES IN DIRECTORY / LOAD INTO "user_data_base" TABLE`);
    console.log(getCurrentDateTime());
    let rowsAdded = 0;
    const directory = `${csvExportPath}user_data`; // Directory containing your CSV files
    console.log(directory);
    // List all files in the directory
    const files = await fs.readdir(directory);
    console.log(files);
    let numberOfFiles = 0;

    // Iterate through each file
    for (let i = 0; i < files.length; i++) {
      let currentFile = files[i];
      
      if (currentFile.endsWith('.csv')) {
        numberOfFiles++;

        // Construct the full file path
        const filePath = `${directory}/${currentFile}`;

        // await read_data(filePath); -- view read file

        // Insert file into "user_data" table
        let query = await execute_insert_user_data_query(pool, filePath, rowsAdded);

        // track number of rows added
        rowsAdded += parseInt(query);
        console.log(`File ${i} of ${files.length}`);
        console.log(`Rows added = ${rowsAdded}\n`);
      }
    }

    generateLogFile('loading_user_data', `Total files added = ${numberOfFiles} Total rows added = ${rowsAdded.toLocaleString()}`, csvExportPath);
    console.log('Files processed = ', numberOfFiles);

    // STEP #5 - INSERT "CREATED AT" DATE
    console.log(`STEP #5 - INSERT "CREATED AT" DATE`);
    await execute_insert_createdAt_query(pool, `${table}`);

  } catch (error) {
    console.error('Catch error:', error);
    generateLogFile('loading_user_data', `Error loading user data: ${error}`, csvExportPath);

  } finally {
      // CLOSE CONNECTION
      await pool.end(err => {
        if (err) {
            console.error('Error closing connection pool:', err.message);
        } else {
            console.log('Connection pool closed successfully.');
        }
      });

      // LOG RESULTS
    const endTime = performance.now();
    const elapsedTime = ((endTime - startTime) / 1_000).toFixed(2); //convert ms to sec

    console.log(`\nAll loading data queries executed successfully. Elapsed Time: ${elapsedTime ? elapsedTime : "Opps error getting time"} sec\n`);

    return elapsedTime;  
  }
}

// Call the function
// execute_load_user_data();

module.exports = {
  execute_load_user_data,
}
