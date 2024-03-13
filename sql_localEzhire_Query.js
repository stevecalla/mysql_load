const mysql = require('mysql2');
const { queryBookingKeyStats } = require('./query_booking_key_stats');

// MySQL configuration
const mysqlConfig = {
    host: 'localhost',
    port: 3306,
    user: 'root',
    password: 'denverdenver',
    database: 'ezhire_booking_data',
    connectionLimit: 10, // adjust as needed
};

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
