const dotenv = require('dotenv');
dotenv.config({path: "../.env"});
const connectionLimitThrottle = 30;

// console.log(process.env); // double check if env variables are available
const forwardConfig = {
    srcHost: '127.0.0.1',
    srcPort: 3306,
    dstHost: process.env.MYSQL_HOST,
    dstPort: parseInt(process.env.MYSQL_PORT),
};

// LEADS DB
const dbConfigLeadsProduction = {
    host: process.env.MYSQL_HOST,
    port: parseInt(process.env.MYSQL_PORT),
    user: process.env.LEADS_USER_PRODUCTION,
    password: process.env.LEADS_PASSWORD_PRODUCTION,
    database: process.env.LEADS_DATABASE_PRODUCTION,
    connectionLimit: connectionLimitThrottle,
    multipleStatements: true // Enable multiple statements
};

const sshConfigLeadsProduction = {
    host: process.env.SSH_HOST_LEADS_PRODUCTION,
    port: parseInt(process.env.SSH_PORT_LEADS_PRODUCTION),
    username: process.env.SSH_USERNAME_LEADS_PRODUCTION,
    password: process.env.SSH_PASSWORD_LEADS_PRODUCTION,
    // privateKey: fs.readFileSync('/path/to/your/private/key'),
};

// BOOKING DB
const dbConfig = {
    host: process.env.MYSQL_HOST,
    port: parseInt(process.env.MYSQL_PORT),
    user: process.env.MYSQL_USER,
    password: process.env.MYSQL_PASSWORD,
    database: process.env.MYSQL_DATABASE,
    connectionLimit: connectionLimitThrottle,
    multipleStatements: true // Enable multiple statements
};

const dbConfigProduction = {
    host: process.env.MYSQL_HOST,
    port: parseInt(process.env.MYSQL_PORT),
    user: process.env.MYSQL_USER_PRODUCTION,
    password: process.env.MYSQL_PASSWORD_PRODUCTION,
    database: process.env.MYSQL_DATABASE_PRODUCTION,
    connectionLimit: connectionLimitThrottle,
    multipleStatements: true // Enable multiple statements
};

const sshConfig = {
    host: process.env.SSH_HOST,
    port: parseInt(process.env.SSH_PORT),
    username: process.env.SSH_USERNAME,
    password: process.env.SSH_PASSWORD,
    // privateKey: fs.readFileSync('/path/to/your/private/key'),
};

const sshConfigProduction = {
    host: process.env.SSH_HOST_PRODUCTION,
    port: parseInt(process.env.SSH_PORT_PRODUCTION),
    username: process.env.SSH_USERNAME_PRODUCTION,
    password: process.env.SSH_PASSWORD_PRODUCTION,
    // privateKey: fs.readFileSync('/path/to/your/private/key'),
};

const localBookingDbConfig = {
    host: process.env.LOCAL_HOST,
    port: 3306,
    user: process.env.LOCAL_MYSQL_USER,
    password: process.env.LOCAL_MYSQL_PASSWORD,
    database: process.env.LOCAL_EZHIRE_BOOKING_DB,
    connectionLimit: connectionLimitThrottle,
    multipleStatements: true // Enable multiple statements
};

const localKeyMetricsDbConfig = {
    host: process.env.LOCAL_HOST,
    port: 3306,
    user: process.env.LOCAL_MYSQL_USER,
    password: process.env.LOCAL_MYSQL_PASSWORD,
    database: process.env.LOCAL_EZHIRE_KEYMETRICS_DB,
    connectionLimit: connectionLimitThrottle,
    multipleStatements: true // Enable multiple statements
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
    connectionLimit: connectionLimitThrottle,
    multipleStatements: true // Enable multiple statements
    // timeout: 60000 //not sure if this works
    // connectTimeout: 10000 //not sure if this works
    // https://github.com/mysqljs/mysql#connection-options
    //https://stackoverflow.com/questions/46756829/node-application-how-to-increase-timeout-for-mysql2-when-debbuging
};

const localLeadDbConfig = {
    host: process.env.LOCAL_HOST,
    port: parseInt(process.env.MYSQL_PORT),
    user: process.env.LOCAL_MYSQL_USER,
    password: process.env.LOCAL_MYSQL_PASSWORD,
    database: process.env.LOCAL_EZHIRE_LEADS_RESPONSE_DB,
    connectionLimit: connectionLimitThrottle,
    multipleStatements: true // Enable multiple statements
};

const localForecastDbConfig = {
    host: process.env.LOCAL_HOST,
    port: parseInt(process.env.MYSQL_PORT),
    user: process.env.LOCAL_MYSQL_USER,
    password: process.env.LOCAL_MYSQL_PASSWORD,
    database: process.env.LOCAL_EZHIRE_FORECAST_DB,
    connectionLimit: connectionLimitThrottle,
    multipleStatements: true // Enable multiple statements
};

const local_mock_rfm_db_config = {
    host: process.env.LOCAL_HOST,
    port: 3306,
    user: process.env.LOCAL_MYSQL_USER,
    password: process.env.LOCAL_MYSQL_PASSWORD,
    // database: process.env.LOCAL_ATTENDANCE_DB,
    connectionLimit: connectionLimitThrottle,
    multipleStatements: true // Enable multiple statements
};

const localUserDbConfig = {
    host: process.env.LOCAL_HOST,
    port: 3306,
    user: process.env.LOCAL_MYSQL_USER,
    password: process.env.LOCAL_MYSQL_PASSWORD,
    database: process.env.LOCAL_EZHIRE_USER_DB,
    connectionLimit: connectionLimitThrottle,
    multipleStatements: true // Enable multiple statements
};

const csvExportPath = `C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/data/`;
// csvExportPath: './output/results.csv', // Update this path accordingly
// csvExportPath: 'C:/Users/calla/Google Drive/Resume & Stuff/ezhire/sql_analysis/data',

module.exports = {
    dbConfig,
    dbConfigProduction,
    dbConfigLeadsProduction,
    sshConfig,
    sshConfigProduction,
    sshConfigLeadsProduction,
    forwardConfig,
    localBookingDbConfig,
    localKeyMetricsDbConfig,
    localPacingDbConfig,
    localUserDbConfig,
    localLeadDbConfig,
    localForecastDbConfig,
    local_mock_rfm_db_config,
    csvExportPath,
    
};