// const dotenv = require('dotenv');
// dotenv.config(); // Load environment variables from .env file

// module.exports = {
//     dbConfig: {
//         host: process.env.MYSQL_HOST,
//         port: parseInt(process.env.MYSQL_PORT),
//         user: process.env.MYSQL_USER,
//         password: process.env.MYSQL_PASSWORD,
//         database: process.env.MYSQL_DATABASE,
//         connectionLimit: 10,
//     },

//     sshConfig: {
//         host: process.env.SSH_HOST,
//         port: parseInt(process.env.SSH_PORT),
//         username: process.env.SSH_USERNAME,
//         password: process.env.SSH_PASSWORD,
//         // privateKey: fs.readFileSync('/path/to/your/private/key'),
//     },

//     forwardConfig: {
//         srcHost: '127.0.0.1',
//         srcPort: 3306,
//         dstHost: process.env.MYSQL_HOST,
//         dstPort: parseInt(process.env.MYSQL_PORT),
//     },

//     localDbConfig: {
//         host: process.env.LOCAL_HOST,
//         user: process.env.LOCAL_MYSQL_USER,
//         password: process.env.LOCAL_MYSQL_PASSWORD,
//         database: process.env.LOCAL_EZHIRE_DB,
//         connectionLimit: 20, // adjust as needed
//     },

//     // csvExportPath: './output/results.csv', // Update this path accordingly
//     // csvExportPath: 'C:/Users/calla/Google Drive/Resume & Stuff/ezhire/sql_analysis/data',
//     csvExportPath: 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/data/',
// };