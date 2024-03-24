const fs = require('fs');
const mysql = require('mysql2');
const { localKeyMetricsDbConfig, } = require('../utilities/config');
const { createLocalDBConnection } = require('../utilities/connectionLocalDB');
const { generate_distinct_list } = require('./query_distinct_keyMetricsCore_031424');
const { generateLogFile } = require('../utilities/generateLogFile');

// Function to execute query for a single date range
async function executeQuery(pool, field, distinctList) {
    return new Promise((resolve, reject) => {

        const query = generate_distinct_list(field);
        // console.log(query);

        const startTime = performance.now();

        pool.query(query, (queryError, results) => {
            const endTime = performance.now();
            const elapsedTime = ((endTime - startTime) / 1_000).toFixed(2); //convert ms to sec

            if (queryError) {
                console.error('Error executing select query:', queryError);
                reject(queryError);

            } else {

                // results.forEach(result => console.log(result));
                results.forEach(result => distinctList.push(result));
                // console.log(`Query results length: ${results.length}, Elapsed Time: ${elapsedTime} sec`);
                resolve();
                return distinctList;
            }
        });
    });
}

// Main function to handle SSH connection and execute queries
async function get_distinct() {
    try {
        const pool = await createLocalDBConnection(localKeyMetricsDbConfig);

        // Array of date ranges to loop through
        const fields = ['vendor', 'booking_type', 'is_repeat', 'country'];

        let distinctList = [];
        // Execute queries for each date range
        for (const field of fields) {
            
            await executeQuery(pool, field, distinctList);

            console.log(`Query for distinct ${field} executed successfully.`);
            // console.log(distinctList);

            generateLogFile('distinct_onrent_list', `Query for ${field} executed successfully.`);
        };

        let finalDistinctList = distinctList.map(item => {
            const [key, value] = Object.entries(item)[0];
            return { segment: key, option: value };
        });

        finalDistinctList.forEach(element => { generateLogFile('distinct_onrent_list', `${element.segment}, ${element.option}`) });

        // Close the pool connection after all queries are executed
        await pool.end(err => {
          if (err) {
            console.error('Error closing connection pool:', err.message);
          } else {
            console.log('Connection pool closed successfully.');
          }
        });

        console.log('All queries executed successfully.');

        return(finalDistinctList);

    } catch (error) {
        console.error('Error:', error);
        
        await pool.end(err => {
          if (err) {
            console.error('Error closing connection pool:', err.message);
          } else {
            console.log('Connection pool closed successfully.');
          }
        });
    }
}

// Run the main function
// get_distinct();

module.exports = {
    get_distinct,
}
