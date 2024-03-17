const fs = require('fs');
const mysql = require('mysql2');
const config = require('../utilities/config');
const { generateLogFile } = require('../utilities/generateLogFile');
const { generateRepeatCode } = require('./generateOnRentSQL_031624');
const { get_distinct } = require('./sql_getDistinct_fields_loop');

// Function to create a Promise for managing the SSH connection and MySQL queries
function createLocalConnection() {
    return new Promise((resolve, reject) => {

        // MySQL configuration
        const mysqlConfig = config.localKeyMetricsDbConfig;

        // Create a MySQL connection pool
        const pool = mysql.createPool(mysqlConfig);

        resolve(pool);
    });
}

// Function to execute query for a single date range
async function executeQuery(pool, distinctList) {
    //create sql query
    const query = await generateRepeatCode(distinctList);
    console.log(query);

    return new Promise((resolve, reject) => {

        // const query = generateRepeatCode(list);
        // console.log(query);

        const startTime = performance.now();

        const dropTable = `DROP TABLE IF EXISTS temp;`;

        pool.query(dropTable, (queryError, results) => {
            if (queryError) {
                console.error('Error executing select query:', queryError);
                reject(queryError);
            } else {
                console.log('\nDrop table results=');
                console.table( results);
                console.log('Drop table results\n');
                // resolve();
            }
        })

        pool.query(query, (queryError, results) => {
            const endTime = performance.now();
            const elapsedTime = ((endTime - startTime) / 1_000).toFixed(2); //convert ms to sec

            if (queryError) {
                console.error('Error executing select query:', queryError);
                reject(queryError);
            } else {
                console.log('\nCreate table results');
                console.table( results);
                console.log('Create table results\n');
                console.log(`Query results length: ${results.length}, Elapsed Time: ${elapsedTime} sec`);
                resolve();
            }
        });
    });
}

// Main function to handle SSH connection and execute queries
async function main() {
    try {
        const pool = await createLocalConnection();

        //get distinct list
        const distinctList = await get_distinct();
        console.log('a', distinctList)

        //send distinct list to the execute function
        await executeQuery(pool, distinctList);

        // generateLogFile('onrent_data', `Query for ${startDate} to ${endDate} executed successfully.`, config.csvExportPath);
        console.log('All queries executed successfully.');
        await pool.end();
    } catch (error) {
        console.error('Error:', error);
    } finally {
        // End the pool
    }
}

// Run the main function
main();
