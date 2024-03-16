const mysql = require('mysql2');
const { queryBookingKeyStats } = require('./query_booking_key_stats');
const config = require('./utilities/config');
// const { generateRepeatCode } = require('../../sql_queries/key_stats_queries/generateOnRentSQL_031624');

// console.log(config);
// console.log(process.env);

// MySQL configuration
const mysqlConfig = config.localDbConfig;

// console.log(config);
// console.log(process.env);
// console.log(mysqlConfig);

// Create a MySQL connection pool
const pool = mysql.createPool(mysqlConfig);

// Example query to select all rows from an example table
// const selectQuery = 'SELECT booking_id FROM booking_data WHERE color LIKE "white";';
const selectQuery = queryBookingKeyStats;

// Execute the query
pool.query(selectQuery, (err, results) => {
  if (err) {
    console.error('Error executing select query:', err);
  } else {
    // console.log('Query results:', results);
    console.table(results);
  }

  // Close the connection pool
  pool.end();
});
