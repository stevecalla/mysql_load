const dotenv = require('dotenv');
dotenv.config({path: "../.env"}); // adding the path ensures each folder will read the .env file as necessary

// console.log(process.env); // double check if env variables are available

const dbConfig = {
    host: process.env.MYSQL_HOST,
    port: parseInt(process.env.MYSQL_PORT),
    user: process.env.MYSQL_USER,
    password: process.env.MYSQL_PASSWORD,
    database: process.env.MYSQL_DATABASE,
    connectionLimit: 10,
};

const sshConfig = {
    host: process.env.SSH_HOST,
    port: parseInt(process.env.SSH_PORT),
    username: process.env.SSH_USERNAME,
    password: process.env.SSH_PASSWORD,
    // privateKey: fs.readFileSync('/path/to/your/private/key'),
};

const forwardConfig = {
    srcHost: '127.0.0.1',
    srcPort: 3306,
    dstHost: process.env.MYSQL_HOST,
    dstPort: parseInt(process.env.MYSQL_PORT),
};

const localBookingDbConfig = {
    host: process.env.LOCAL_HOST,
    port: 3306,
    user: process.env.LOCAL_MYSQL_USER,
    password: process.env.LOCAL_MYSQL_PASSWORD,
    database: process.env.LOCAL_EZHIRE_BOOKING_DB,
    connectionLimit: 20, // adjust as needed
};

const localKeyMetricsDbConfig = {
    host: process.env.LOCAL_HOST,
    port: 3306,
    user: process.env.LOCAL_MYSQL_USER,
    password: process.env.LOCAL_MYSQL_PASSWORD,
    database: process.env.LOCAL_EZHIRE_KEYMETRICS_DB,
    connectionLimit: 20, // adjust as needed
    // timeout: 60000 //not sure if this works
    // connectTimeout: 10000 //not sure if this works
    // https://github.com/mysqljs/mysql#connection-options
    //https://stackoverflow.com/questions/46756829/node-application-how-to-increase-timeout-for-mysql2-when-debbuging
};

const localPacingDbConfig = {
    host: process.env.LOCAL_HOST,
    port: 3306,
    user: process.env.LOCAL_MYSQL_USER,
    password: process.env.LOCAL_MYSQL_PASSWORD,
    database: process.env.LOCAL_EZHIRE_PACING_DB,
    connectionLimit: 20, // adjust as needed
    // timeout: 60000 //not sure if this works
    // connectTimeout: 10000 //not sure if this works
    // https://github.com/mysqljs/mysql#connection-options
    //https://stackoverflow.com/questions/46756829/node-application-how-to-increase-timeout-for-mysql2-when-debbuging
};

const localUserDbConfig = {
    host: process.env.LOCAL_HOST,
    port: 3306,
    user: process.env.LOCAL_MYSQL_USER,
    password: process.env.LOCAL_MYSQL_PASSWORD,
    database: process.env.LOCAL_EZHIRE_USER_DB,
    connectionLimit: 20,
};

const csvExportPath = `C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/data/`;
// csvExportPath: './output/results.csv', // Update this path accordingly
// csvExportPath: 'C:/Users/calla/Google Drive/Resume & Stuff/ezhire/sql_analysis/data',

module.exports = {
    dbConfig,
    sshConfig,
    forwardConfig,
    localBookingDbConfig,
    localKeyMetricsDbConfig,
    localPacingDbConfig,
    localUserDbConfig,
    csvExportPath,
};