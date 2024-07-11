// Source below. The code is a bit differnt to break into pieces for config, query
// https://medium.com/swlh/node-js-how-to-access-mysql-remotely-using-ssh-d45e21221039
// https://stackoverflow.com/questions/73111127/can-connect-to-local-mysql-database-on-remote-server-through-workbench-but-not-n

const fs = require('fs');
const mysql = require('mysql2');
const { Client } = require('ssh2');
const sshClient = new Client();
const config = require('./config');
const { queryAllPayments } = require('../query_AllPayments'); // Import the selectQuery from query_AllPayments.js
const { queryBookingData } = require('./query_BookingData'); // Import the selectQuery from query_BookingData.js

// Create a Promise for managing the SSH connection and MySQL queries
const SSHConnection = new Promise((resolve, reject) => {
    // Set up SSH tunnel
    sshClient.on('ready', () => {
        console.log('\nSSH tunnel established.\n');

        // Establish TCP connection through the SSH tunnel
        sshClient.forwardOut(
            config.forwardConfig.srcHost,
            config.forwardConfig.srcPort,
            config.forwardConfig.dstHost,
            config.forwardConfig.dstPort,
            (err, stream) => {
                if (err) reject(err);

                // Update MySQL server configuration with the established stream and SSL settings
                const updatedDbServer = {
                    // ...mySQLDbServer,
                    ...config.dbConfig,
                    stream,
                    ssl: {
                        // Enable SSL for secure transport
                        rejectUnauthorized: false, // You might want to set this to true in a production environment
                    },
                };

                // Create a MySQL connection pool
                const pool = mysql.createPool(updatedDbServer);

                // Obtain a connection from the pool
                pool.getConnection((error, connection) => {
                    if (error) {
                        reject(error);
                    } else {
                        // Execute your MySQL queries using the obtained connection
                        // const selectQuery = 'SELECT * FROM all_payments_all LIMIT 1;';

                        // START ------------------------------
                        // RUN TEST QUERY ON THE ALL PAYMENT TABLE
                        // Use the imported queryAllPayments
                        const selectQuery = queryAllPayments;
                        // END *********************************
                        
                        
                        // START ------------------------------
                        // // Replace the searchString with the actual value you want to use for filtering
                        // const startDate = '2024-01-25';
                        // const endDate = '2024-01-25';
                        
                        // // Replace 'startDateVariable' and 'endDateVariable
                        // const modifiedBookingQuery = queryBookingData
                        //     .replace(
                        //         `startDateVariable`,
                        //         `${startDate}`
                        //     ).replace(
                            //         `endDateVariable`,
                        //         `${endDate}`
                        //     );

                        // // const selectQuery = queryBookingData;
                        // // console.log(selectQuery);
                        // const selectQuery = modifiedBookingQuery;
                        // console.log(modifiedBookingQuery);
                        // END *********************************
                        
                        // Execute the query
                        connection.query(selectQuery, (queryError, results) => {
                            if (queryError) {
                                console.error('Error executing select query:', queryError);
                            } else {
                                console.log('Query results:', results);
                                console.log('Query results length:', results.length);
                                exportResultsToCSV(results);
                            }

                            // Release the connection back to the pool
                            connection.release();

                            // Close the SSH tunnel
                            sshClient.end();

                            // Resolve the promise
                            resolve();
                        });
                    }
                });
            });
    }).connect(config.sshConfig);
});

// Usage of the SSHConnection promise
SSHConnection
    .then(execute_SSH_Connection = true)
    .then((results) => {
        console.log('SSH tunnel established and query executed successfully.');
    })
    .catch((error) => {
        console.error('Error:', error);
    });


// Function to export results to a CSV file
function exportResultsToCSV(results) {
    if (results.length === 0) {
        console.log('No results to export.');
        return;
    }

    const header = Object.keys(results[0]); // set header row equal to the object keys
    const csvContent = `${header.join(',')}\n${results.map(row => header.map(key => row[key]).join(',')).join('\n')}`; // map results to each row

    fs.writeFileSync(config.csvExportPath, csvContent);
    console.log(`Results exported to ${config.csvExportPath}`);
}

// Example usage
// results = [
//     {
//         Booking_ID: '22919',
//         agreement_number: '',
//         vendor_id: 2416,
//         Vendor_Name: 'reservations-budget',
//         Car_Booked: 'Chevrolet Malibu or Similar',
//         Car_Assigned: 'Malibu',
//         vrates: '100',
//         our_rate: '139',
//         Rent_Out_Date: '25-Jan-2020',
//         Rent_In_Date: '26-Jan-2020',
//         daysc: '1',
//         Rental_cost: '100',
//         Delivery_cost: '0',
//         Collection_cost: '30',
//         ins_cost: '0',
//         BS_cost: '0',
//         GPS_cost: '0',
//         BOS_cost: '0',
//         PAI_cost: '0',
//         AD_cost: '0',
//         Total_Rental_Cost_without_Vat: '130',
//         Total_Rental_Cost_with_VAT: '136.5',
//         Period: '20Jan_26Jan_2020'
//     },
//     {
//         Booking_ID: '22919',
//         agreement_number: '',
//         vendor_id: 2416,
//         Vendor_Name: 'reservations-budget',
//         Car_Booked: 'Chevrolet Malibu or Similar',
//         Car_Assigned: 'Malibu',
//         vrates: '100',
//         our_rate: '139',
//         Rent_Out_Date: '25-Jan-2020',
//         Rent_In_Date: '26-Jan-2020',
//         daysc: '1',
//         Rental_cost: '100',
//         Delivery_cost: '0',
//         Collection_cost: '30',
//         ins_cost: '0',
//         BS_cost: '0',
//         GPS_cost: '0',
//         BOS_cost: '0',
//         PAI_cost: '0',
//         AD_cost: '0',
//         Total_Rental_Cost_without_Vat: '130',
//         Total_Rental_Cost_with_VAT: '136.5',
//         Period: '20Jan_26Jan_2020'
//     },
// ];

// exportResultsToCSV(results);