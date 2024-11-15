const fs = require('fs');
const dotenv = require('dotenv');
dotenv.config({ path: "../.env" }); // adding the path ensures each folder will read the .env file as necessary

const mysql = require('mysql2');
const { Client } = require('ssh2');
const sshClient = new Client();

const { forwardConfig, dbConfigLeadsProduction, sshConfigLeadsProduction  } = require('../utilities/config');

const { query_lead_stats } = require('./query_lead_stats_111424');

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

// STEP #1: GET / QUERY DAILY BOOKING DATA & RETURN RESULTS
async function execute_query_get_daily_lead_data(pool) {
    return new Promise((resolve, reject) => {

        const startTime = performance.now();

        const query = query_lead_stats();
        // console.log(query);

        pool.query(query, (queryError, results) => {
            const endTime = performance.now();
            const elapsedTime = ((endTime - startTime) / 1_000).toFixed(2); //convert ms to sec

            if (queryError) {
                console.error('Error executing select query:', queryError);
                reject(queryError);
            } else {

                // console.table(results[0]);
                console.log('***** DISPLAYING FIRST 10 RESULTS');
                // console.table(results.slice(0, 10));
                console.table(results);
                console.log(results);
                console.log(`Query results length: ${results.length}, Elapsed Time: ${elapsedTime} sec`);


                resolve(results);
            }
        });
    });
}

async function execute_get_daily_lead_data(is_development_pool) {
    let pool;
    let results;
    const startTime = performance.now();

    try {
        // STEP #1: GET / QUERY Booking data DATA & RETURN RESULTS
        pool = await createSSHConnection(is_development_pool);
        results = await execute_query_get_daily_lead_data(pool);

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
execute_get_daily_lead_data();

module.exports = {
    execute_get_daily_lead_data,
}