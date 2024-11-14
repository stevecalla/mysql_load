const fs = require('fs');
const dotenv = require('dotenv');
dotenv.config({ path: "../.env" }); // adding the path ensures each folder will read the .env file as necessary

const mysql = require('mysql2');
const { Client } = require('ssh2');
const sshClient = new Client();

const { forwardConfig, dbConfig, sshConfig, dbConfigProduction, sshConfigProduction  } = require('../utilities/config');

const { query_booking_count_today_v2 } = require('./query_booking_count_today_v2');

// Function to create a Promise for managing the SSH connection and MySQL queries
function createSSHConnection(is_development_pool = true) {

    const { srcHost, srcPort, dstHost, dstPort } = forwardConfig;
    const db = is_development_pool ? dbConfig : dbConfigProduction;
    const ssh = is_development_pool ? sshConfig : sshConfigProduction;

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

// STEP #1: GET / QUERY DAILY BOOKING DATA & RETURN RESULTS
async function execute_query_get_daily_booking_data(pool) {
    return new Promise((resolve, reject) => {

        const startTime = performance.now();

        const query = query_booking_count_today_v2();
        // console.log(query);

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

                resolve(results);
            }
        });
    });
}

async function execute_get_daily_booking_data(is_development_pool) {
    let pool;
    let results;
    const startTime = performance.now();

    try {
        // STEP #1: GET / QUERY Booking data DATA & RETURN RESULTS
        pool = await createSSHConnection(is_development_pool);
        results = await execute_query_get_daily_booking_data(pool);

        // Return the results from the try block
        return results;

    } catch (error) {
        // Handle errors
        console.error('Error:', error);
        throw error;  // Optionally re-throw the error if you want to propagate it further

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

        console.log(`\nAll get booking data queries executed successfully. Elapsed Time: ${elapsedTime ? elapsedTime : "Oops error getting time"} sec\n`);
    }
}

// Run the main function
// execute_get_daily_booking_data();

module.exports = {
    execute_get_daily_booking_data,
}