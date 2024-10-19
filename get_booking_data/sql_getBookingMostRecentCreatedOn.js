// const fs = require('fs');
// const mysql = require('mysql2');

// const { Client } = require('ssh2');
// const sshClient = new Client();
// const { forwardConfig , dbConfig, sshConfig, csvExportPath } = require('../utilities/config');

// const { query_most_recent_create_on_date} = require('./query_most_recent_created_on');
// const { generateLogFile } = require('../utilities/generateLogFile');

// // console.log('process env', process.env);
// // console.log('sshConfig', sshConfig);

// // Function to create a Promise for managing the SSH connection and MySQL queries
// function createSSHConnection() {
//     return new Promise((resolve, reject) => {
//         sshClient.on('ready', () => {
//             console.log('SSH tunnel established.\n');

//             const { srcHost, srcPort, dstHost, dstPort } = forwardConfig;
//             sshClient.forwardOut(
//                 srcHost,
//                 srcPort,
//                 dstHost,
//                 dstPort,
//                 (err, stream) => {
//                     if (err) reject(err);

//                     const updatedDbServer = {
//                         ...dbConfig,
//                         stream,
//                         ssl: {
//                             rejectUnauthorized: false,
//                         },
//                     };

//                     const pool = mysql.createPool(updatedDbServer);

//                     resolve(pool);
//                 }
//             );
//         }).connect(sshConfig);
//     });
// }

// // Function to execute query for a single date range
// async function execute_query_most_recent_created_on(pool, startDate, endDate) {
//     return new Promise((resolve, reject) => {

//         const startTime = performance.now();

//         const query = query_most_recent_create_on_date;

//         pool.query(query, (queryError, results) => {
//             const endTime = performance.now();
//             const elapsedTime = ((endTime - startTime) / 1_000).toFixed(2); //convert ms to sec

//             if (queryError) {
//                 console.error('Error executing select query:', queryError);
//                 reject(queryError);
//             } else {
//                 let log_message = `Query results length: ${results.length}, Elapsed Time: ${elapsedTime} sec`;
//                 let { last_updated, execution_timestamp, time_stamp_difference, is_within_2_hours } = results[0];

//                 let log_results = `LAST UPDATED: ${last_updated}, EXECUTION TIMESTAMP:${execution_timestamp}, TIME STAMP DIFFERENCE: ${time_stamp_difference}, IS WITHIN 2 HOURS: ${is_within_2_hours}`;

//                 // console.log(log_message);
//                 // console.log(results);

//                 generateLogFile('booking_data', log_message, csvExportPath);
//                 generateLogFile('booking_data', log_results, csvExportPath);

//                 resolve(results);
//             }
//         });
//     });
// }

// // Main function to handle SSH connection and execute queries
// async function execute_get_most_recent_created_on_date() {
//     try {
//         const startTime = performance.now();

//         const pool = await createSSHConnection();

//         let results = await execute_query_most_recent_created_on(pool);   
        
//         // console.log(results);

//         // Close the SSH connection after all queries are executed
//         // sshClient.end(); //revised below to include error messaging
//         sshClient.end(err => {
//             if (err) {
//               console.error('Error closing SSH connection pool:', err.message);
//             } else {
//               console.log('SSH Connection pool closed successfully.');
//             }
//           });
  
//           await pool.end(err => {
//             if (err) {
//               console.error('Error closing connection pool:', err.message);
//             } else {
//               console.log('Connection pool closed successfully.');
//             }
//           });

//           const endTime = performance.now();
//           const elapsedTime = ((endTime - startTime) / 1_000).toFixed(2); //convert ms to sec

//           return { elapsedTime, results };

//     } catch (error) {
//         console.error('Error:', error);

//     }
// }

// // Run the main function
// // execute_get_most_recent_created_on_date();

// module.exports = {
//     execute_get_most_recent_created_on_date,
// }