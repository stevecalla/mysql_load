const mysql = require('mysql2');
const config = require('../utilities/config');
const { generateRepeatCode } = require('./generateOnRentSQL_031624');

// console.log(generateRepeatCode());

// console.log(process.env);

// MySQL configuration
const mysqlConfig = config.localKeyMetricsDbConfig;

// Create a MySQL connection pool
const pool = mysql.createPool(mysqlConfig);

(async () => {
    try {
        // Wait for generateRepeatCode() to complete and get the query
        const selectQuery = generateRepeatCode();
        // console.log(selectQuery);
        // console.log(typeof(selectQuery));

        // Execute the query
        console.time('query execution');

        pool.query(selectQuery, (err, results) => {
            if (err) {
                console.error('Error executing select query:', err);
            } else {
                console.log('Query results:', results);
                console.table(results);
            }

            // Close the connection pool
            // pool.end();

            console.timeLog("query execution");

            const tempTable = 'SELECT * FROM temp';

            pool.query(tempTable, (err, results) => {
                if (err) {
                    console.error('Error executing select query:', err);
                } else {
                    console.log('Query results:', results[0].calendar_date);
                    console.table(results);
                    // console.log(JSON.parse(JSON.stringify(results)));
                }
    
                // Close the connection pool
                pool.end();
    
                console.timeLog("query execution");
            });
            
        });

    } catch (error) {
        console.error('Error generating repeat code:', error);
    }
})();
