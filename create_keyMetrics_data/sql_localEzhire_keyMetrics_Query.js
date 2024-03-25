const mysql = require('mysql2');
const { localKeyMetricsDbConfig, localPacingDbConfig } = require('../utilities/config');
const { createLocalDBConnection } = require('../utilities/connectionLocalDB');
const { generateRepeatCode } = require('./generateOnRentSQL_031624');

(async () => {

  // const pool = await createLocalDBConnection(localKeyMetricsDbConfig);
  const pool = await createLocalDBConnection(localPacingDbConfig);
    console.log(pool.config.connectionConfig.user, pool.config.connectionConfig.database);

    try {
        // const query = 'SELECT * FROM calendar_table LIMIT 1';

        // TESTING A SMALL SCALE VERSION OF mysql_queries/pacing/step2b_pacing_create_pickup_rollup_optimized_032424.sql
        const query = `
          -- CREATE TEMPORARY TABLE IF NOT EXISTS temp_3_running_total_booking_count AS
            SELECT
                pickup_month_year,
                booking_date,
                days_from_first_day_of_month,
                count,
                @running_total := @running_total + count AS running_total_booking_count
            FROM (
                SELECT
                    pb.pickup_month_year,
                    pb.booking_date,
                    pb.days_from_first_day_of_month,
                    SUM(count) AS count
                FROM ezhire_pacing_metrics.pacing_base pb
                GROUP BY 
                    pb.pickup_month_year,
                    pb.booking_date,  
                    pb.days_from_first_day_of_month
                ORDER BY pb.pickup_month_year ASC
                LIMIT 10
            ) AS subquery
            JOIN (SELECT @running_total := 0) AS init;
        `;

        // Execute the query
        console.time('query execution');

        pool.query(query, (err, results) => {
            if (err) {
                console.error('Error executing select query:', err);
            } else {
                console.log('Query results:', results);
                console.table(results);
            }

            console.timeLog("query execution");

            // Close the connection pool
            pool.end(err => {
              if (err) {
                console.error('Error closing connection pool:', err.message);
              } else {
                console.log('Connection pool closed successfully.');
              }
            });
        });

    } catch (error) {
        console.error('Error generating repeat code:', error);
    }
})();
