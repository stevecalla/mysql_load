const mysql = require('mysql2');
const { queryBookingKeyStats } = require('./query_booking_key_stats');
const { localBookingDbConfig } = require('./utilities/config');

// MySQL configuration
const mysqlConfig = localBookingDbConfig;

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
    console.table('Query results:', results);
  }

  // Close the connection pool
  pool.end();
});
