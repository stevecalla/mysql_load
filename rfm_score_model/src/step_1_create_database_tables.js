const mysql = require('mysql2');
const dotenv = require('dotenv');
dotenv.config({ path: "../../.env" }); // add path to read.env file

const { local_mock_rfm_db_config } = require('../../utilities/config');
const { createLocalDBConnection } = require('../../utilities/connectionLocalDB');

const { query_create_database } = require('../queries/queries_create_db');
const { query_drop_database } = require('../queries/queries_drop_db_tables');
const { tables_library } = require('../queries/queries_create_tables');
const { query_insert_seed_data } = require('../queries/queries_insert_seed_data');
const { seed_data } = require('../queries/queries_seed_data');

// Connect to MySQL
async function create_connection() {
    try {
        // Create a connection to MySQL
        const config_details = local_mock_rfm_db_config;
        // console.log(config_details);

        const pool = createLocalDBConnection(config_details);
        // console.log(pool);

        return (pool);
    } catch (error) {
        console.log(`Error connecting: ${error}`)
    }
}

// EXECUTE MYSQL TO CREATE DB QUERY
async function execute_mysql_create_db_query(pool, query, step_info) {
    return new Promise((resolve, reject) => {

        const startTime = performance.now();

        pool.query(query, (queryError, results) => {
            const endTime = performance.now();
            const elapsedTime = ((endTime - startTime) / 1_000).toFixed(2); //convert ms to sec

            if (queryError) {
                console.error('Error executing select query:', queryError);
                reject(queryError);
            } else {
                console.log(`\n${step_info}`);
                console.table(results);
                console.log(`Query results: ${results.info}, Elapsed Time: ${elapsedTime} sec\n`);
                resolve();
            }
        });
    });
}

// EXECUTE MYSQL TO CREATE TABLES & WORK WITH TABLES QUERY
async function execute_mysql_working_query(pool, db_name, query, step_info) {
    return new Promise((resolve, reject) => {

        const startTime = performance.now();

        pool.query(`USE ${db_name};`, (queryError, results) => {
            pool.query(query, (queryError, results) => {
                const endTime = performance.now();
                const elapsedTime = ((endTime - startTime) / 1_000).toFixed(2); //convert ms to sec

                if (queryError) {
                    console.error('Error executing select query:', queryError);
                    reject(queryError);
                } else {
                    console.log(`\n${step_info}`);
                    console.table(results);
                    console.log(`Query results: ${results.info}, Elapsed Time: ${elapsedTime} sec\n`);
                    resolve();
                }
            });
        });
    });
}

// INSERT "CREATED AT" DATE, INSERT "UPDATED AT" DATA
async function execute_insert_createdAt_query(pool, db_name, table, step) {
    return new Promise((resolve, reject) => {

        const startTime = performance.now();

        const addCreateAtDate = `
            ALTER TABLE ${table} 
                ADD COLUMN created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                ADD COLUMN updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP;`;

        pool.query(`USE ${db_name};`, (queryError, results) => {
            pool.query(addCreateAtDate, (queryError, results) => {
                const endTime = performance.now();
                const elapsedTime = ((endTime - startTime) / 1_000).toFixed(2); //convert ms to sec

                if (queryError) {
                    console.error(`Error executing ${step}:`, queryError);
                    reject(queryError);
                } else {
                    console.log(`\n${step}`);
                    console.table(results);
                    console.log(`Query results: ${results.info}, Elapsed Time: ${elapsedTime} sec\n`);

                    // resolve();
                }
            });
        });

        // Update the created_at and updated_at columns to UTC timestamps
        pool.query(`USE ${db_name};`, (queryError, results) => {
            pool.query(`
                UPDATE ${table}
                SET created_at = UTC_TIMESTAMP()
                    -- updated_at = UTC_TIMESTAMP()
                WHERE your_condition;
            `);

            resolve();
        });
    });
}

// INSERT "CREATED AT" DATE, INSERT "UPDATED AT" DATE
async function execute_insert_createdAt_query(pool, db_name, table, step) {
    return new Promise((resolve, reject) => {

        const startTime = performance.now();

        // Get the current date and time in UTC
        const currentUtcDateTime = new Date().toISOString().slice(0, 19).replace('T', ' ');

        // Your SQL query with the UTC timestamp variable
        const addCreateAtDate = `
            ALTER TABLE ${table} 
                ADD COLUMN created_at TIMESTAMP DEFAULT '${currentUtcDateTime}',
                ADD COLUMN updated_at TIMESTAMP DEFAULT '${currentUtcDateTime}' ON UPDATE CURRENT_TIMESTAMP;
        `;

        pool.query(`USE ${db_name};`, (queryError, results) => {
            pool.query(addCreateAtDate, (queryError, results) => {
                const endTime = performance.now();
                const elapsedTime = ((endTime - startTime) / 1_000).toFixed(2); //convert ms to sec

                if (queryError) {
                    console.error(`Error executing ${step}:`, queryError);
                    reject(queryError);
                } else {
                    console.log(`\n${step}`);
                    console.table(results);
                    console.log(`Query results: ${results.info}, Elapsed Time: ${elapsedTime} sec\n`);

                    resolve();
                }
            });
        });
    });
}

async function main() {
    let pool; // Declare the pool variable outside the try block to close connection in finally block
    const startTime = performance.now();

    try {

        // STEP #0: CREATE CONNECTION
        pool = await create_connection();

        const db_name = `mock_rfm_db`;

        // STEP #1: CREATE DATABASE
        await execute_mysql_working_query(pool, db_name, query_drop_database(db_name), `STEP #1.0: DROP DB`);
        await execute_mysql_create_db_query(pool, query_create_database(db_name), `STEP #1.1: CREATE DATABASE`);

        // STEP #2: CREATE TABLES
        for (const table of tables_library) {
            const {table_name, create_query, step, step_info} = table;

            const drop_query = query_drop_database(table_name.toUpperCase());

            const drop_info = `${step} DROP ${step_info.toUpperCase()} TABLE`;
            const create_info = `${step} CREATE ${step_info.toUpperCase()} TABLE`;

            await execute_mysql_working_query(pool, db_name, drop_query, drop_info);
            await execute_mysql_working_query(pool, db_name, create_query, create_info);
        }

        // // STEP #3: SEED TABLES
        for (const data of seed_data) { 
            const { table_name, ...filtered_data } = data; // Destructure the object to exclude the table_name

            const columns = Object.keys(filtered_data).join(', '); // Get column names

            const values = Object.values(filtered_data).map(val => typeof val === 'string' ? `'${val}'` : val).join(', '); // Get values

            const insert_query = query_insert_seed_data(table_name, columns, values);

            await execute_mysql_working_query(pool, db_name, insert_query, `INSERT ${table_name.toUpperCase()} TABLE`);
        }

        console.log('Seed data inserted successfully.');

        // STEP #4: UPDATE TABLES TO INCLUDE A CREATED AT AND UPDATED AT FIELD/DATE
        for (const table of tables_library) {
            const { table_name } = table;

            await execute_insert_createdAt_query(pool, db_name, table_name, `STEP #5: INSERT CREATED/UPDATED AT DATE IN ${table_name.toUpperCase()} TABLE`);
        }

        // STEP #5a: Log results
        console.log('STEP #5A: All queries executed successfully.');

    } catch (error) {
        // STEP #5b: Log results
        console.log('STEP #5B: All queries NOT executed successfully.');

        console.error('Error:', error);

    } finally {
        // STEP #5c: Log results
        const endTime = performance.now();
        const elapsedTime = ((endTime - startTime) / 1_000).toFixed(2); //convert ms to sec
        console.log(`\nSTEP #5C = TIME LOG. Elapsed Time: ${elapsedTime ? elapsedTime : "Opps error getting time"} sec\n`);
        // return elapsedTime;

        // STEP #6: CLOSE CONNECTION/POOL
        await pool.end(err => {
            if (err) {
                console.error('Error closing connection pool:', err.message);
            } else {
                console.log('Connection pool closed successfully.');
            }
        });
    }
}

main();
