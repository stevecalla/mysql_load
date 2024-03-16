const mysql = require('mysql2');
const { queryBookingKeyStats } = require('./query_booking_key_stats');
const config = require('./utilities/config');
const { generateRepeatCode } = require('../sql_queries/key_stats_queries/generateOnRentSQL_031624');

// console.log(generateRepeatCode());

// MySQL configuration
const mysqlConfig = config.localKeyMetricsDbConfig;

// Create a MySQL connection pool
const pool = mysql.createPool(mysqlConfig);

(async () => {
    try {
        // Wait for generateRepeatCode() to complete and get the query
        const selectQuery = await generateRepeatCode();
        // console.log(selectQuery);
        // console.log(typeof(selectQuery));

        // Execute the query
        console.time('query execution');

        // get a timestamp before running the query
        var pre_query = new Date().getTime();

        pool.query(selectQuery, (err, results) => {
            if (err) {
                console.error('Error executing select query:', err);
            } else {
                // console.log('Query results:', results);
                console.table(results);
            }

            // Close the connection pool
            pool.end();

            console.timeLog("query execution");

        });

        
    } catch (error) {
        console.error('Error generating repeat code:', error);
    }
})();
