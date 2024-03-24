const mysql = require('mysql2');
const { localKeyMetricsDbConfig } = require('../utilities/config');
const { createLocalDBConnection } = require('../utilities/connectionLocalDB');
const { generateRepeatCode } = require('./generateOnRentSQL_031624');

(async () => {

    const pool = await createLocalDBConnection(localKeyMetricsDbConfig);
    console.log(pool.config.connectionConfig.user, pool.config.connectionConfig.database);

    try {
        const query = 'SELECT * FROM calendar_table LIMIT 1';

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
