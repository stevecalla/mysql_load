// const mysql = require('mysql');
const mysql = require('mysql2');
const { localPacingDbConfig, csvExportPath } = require('../utilities/config');
const { createLocalDBConnection } = require('../utilities/connectionLocalDB');

// Create a MySQL connection pool
const pool = mysql.createPool({
    host: process.env.LOCAL_HOST,
    port: 3306,
    user: process.env.LOCAL_MYSQL_USER,
    password: process.env.LOCAL_MYSQL_PASSWORD,
    database: process.env.LOCAL_EZHIRE_PACING_DB,
    connectionLimit: 20,
});

// Acquire a connection from the pool
pool.getConnection((error, connection) => {
  if (error) {
    console.error('Error acquiring connection from pool:', error);
    return;
  }

  // Begin a transaction
  connection.beginTransaction((beginErr) => {
    if (beginErr) {
      console.error('Error beginning transaction:', beginErr);
      connection.release();
      return;
    }

    // Drop the table if it exists
    connection.query('DROP TABLE IF EXISTS pacing_base_groupby_v2;', (dropErr) => {
      if (dropErr) {
        return connection.rollback(() => {
          console.error('Error dropping table:', dropErr);
          connection.release();
        });
      }

      // Initialize variables within the transaction using SET statements
      connection.query('SET @running_total_booking_count = 0;', (setQueryErr1) => {
        if (setQueryErr1) {
          return connection.rollback(() => {
            console.error('Error setting variable:', setQueryErr1);
            connection.release();
          });
        }

        // Continue initializing other variables
        connection.query('SET @running_total_booking_charge_aed = 0;', (setQueryErr2) => {
          if (setQueryErr2) {
            return connection.rollback(() => {
              console.error('Error setting variable:', setQueryErr2);
              connection.release();
            });
          }

          // Execute the main query within the transaction
          connection.query(`
            CREATE TABLE pacing_base_groupby_v2 AS
            SELECT
                pickup_month_year,
                booking_date,
                days_from_first_day_of_month,
                count,
                FORMAT(booking_charge_aed, 0) AS total_booking_charge_aed,
                FORMAT(booking_charge_less_discount_aed, 0) AS total_booking_charge_less_discount_aed,
                FORMAT(booking_charge_less_discount_extension_aed, 0) AS total_booking_charge_less_discount_extension_aed,
                FORMAT(extension_charge_aed, 0) AS total_extension_charge_aed,
                FORMAT(
                    @running_total_booking_count := IF(@prev_month_year = pickup_month_year,
                        @running_total_booking_count + count,
                        count
                    ), 0
                ) AS running_total_booking_count,
                FORMAT(
                    @running_total_booking_charge_aed := IF(@prev_month_year = pickup_month_year,
                        @running_total_booking_charge_aed + booking_charge_aed,
                        booking_charge_aed
                    ), 0
                ) AS running_total_booking_charge_aed,
                FORMAT(
                    @running_total_booking_charge_less_discount_aed := IF(@prev_month_year = pickup_month_year,
                        @running_total_booking_charge_less_discount_aed + booking_charge_less_discount_aed,
                        booking_charge_less_discount_aed
                    ), 0
                ) AS running_total_booking_charge_less_discount_aed,
                FORMAT(
                    @running_total_booking_charge_less_discount_extension_aed := IF(@prev_month_year = pickup_month_year,
                        @running_total_booking_charge_less_discount_extension_aed + booking_charge_less_discount_extension_aed,
                        booking_charge_less_discount_extension_aed
                    ), 0
                ) AS running_total_booking_charge_less_discount_extension_aed,
                FORMAT(
                    @running_total_extension_charge_aed := IF(@prev_month_year = pickup_month_year,
                        @running_total_extension_charge_aed + extension_charge_aed,
                        extension_charge_aed
                    ), 0
                ) AS running_total_extension_charge_aed,
                @prev_month_year := pickup_month_year AS dummy_variable
            FROM (
                SELECT
                    pb.pickup_month_year,
                    pb.booking_date,
                    pb.days_from_first_day_of_month,
                    SUM(count) AS count,
                    SUM(booking_charge_aed) AS booking_charge_aed,
                    SUM(booking_charge_less_discount_aed) AS booking_charge_less_discount_aed,
                    SUM(booking_charge_less_discount_extension_aed) AS booking_charge_less_discount_extension_aed,
                    SUM(extension_charge_aed) AS extension_charge_aed
                FROM ezhire_pacing_metrics.pacing_base pb
                GROUP BY 
                    pb.pickup_month_year,
                    pb.booking_date,  
                    pb.days_from_first_day_of_month
                ORDER BY pb.pickup_month_year ASC
                -- LIMIT 1000
            ) AS subquery;
          `, (queryErr, results) => {
            if (queryErr) {
              return connection.rollback(() => {
                console.error('Error executing main query:', queryErr);
                connection.release();
              });
            }

            // Fetch warnings after executing the main query
            connection.query('SHOW WARNINGS;', (warningsErr, warnings) => {
              if (warningsErr) {
                console.error('Error fetching warnings:', warningsErr);
                connection.release();
                return;
              }

              console.log('Warnings:', warnings);

              // Commit the transaction if everything was successful
              connection.commit((commitErr) => {
                if (commitErr) {
                  return connection.rollback(() => {
                    console.error('Error committing transaction:', commitErr);
                    connection.release();
                  });
                }

                console.log('Transaction committed successfully');

                // Release the connection back to the pool
                connection.release();
              });
            });
          });
        });
      });
    });
  });
});
