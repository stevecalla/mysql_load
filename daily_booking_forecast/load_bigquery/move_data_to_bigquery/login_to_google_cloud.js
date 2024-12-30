// const { exec } = require('child_process');
// const { generateLogFile } = require('../../utilities/generateLogFile');

// const GOOGLE_CLOUD_ACCOUNT_EZHIRE = 'steve@ezhire.life';
// const GOOGLE_CLOUD_PROJECT_ID_EZHRE = 'cool-ship-12345';

// const GOOGLE_CLOUD_ACCOUNT_CALLA = 'callasteven@gmail.com';
// const GOOGLE_CLOUD_PROJECT_ID_CALLA = 'centered-loader-246515';

// // ASYNC FUNCTION TO UPLOAD CSV FILES TO GOOGLE CLOUD STORAGE
// async function execute_login_to_google_cloud() {
//     try {
//         const startTime = performance.now();
        
//         // const command = `gsutil cp "${localFilePath}" ${destinationPath}`;
//         const command_login = `gcloud config set account ${GOOGLE_CLOUD_ACCOUNT}`;
//         const command_set_project_id = `gcloud config set project ${GOOGLE_CLOUD_PROJECT_ID}`;
//         const command_get_projects_list = `gcloud projects list`;
//         const command_get_current_default_project = `gcloud config get-value project`;
//         // console.log(command);

//         // AWAIT EXECUTION OF GSUTIL CP COMMAND
//         await new Promise((resolve, reject) => {
//             // exec(command_get_projects_list, (error, stdout, stderr) => {
//             // exec(command_get_projects_list, (error, stdout, stderr) => {
//             exec(command_set_project_id, (error, stdout, stderr) => {
//                 if (error) {
//                     console.error('Error:', error);
//                     reject(error); // REJECT THE PROMISE IF THERE'S AN ERROR
//                     return;
//                 }

//                 console.log('Login successfully.');
//                 console.log('stdout:', stdout);
//                 console.error('stderr:', stderr);

//                 // GENERATE LOG FILE FOR SUCCESSFUL UPLOAD
//                 generateLogFile('login_to_google_cloud', `Login successfully. ${stdout} ${stderr}`);
//                 resolve(); // RESOLVE THE PROMISE AFTER UPLOAD COMPLETES
//             });
//         });

//         const endTime = performance.now();
//         const elapsedTime = ((endTime - startTime) / 1000).toFixed(2); // CONVERT MS TO SEC
//         return elapsedTime; // RETURN ELAPSED TIME AFTER ALL UPLOADS COMPLETE
//     }

//     catch (error) {
//         console.error('Error:', error);
//         throw error; // THROW ERROR IF AN ERROR OCCURS DURING UPLOAD PROCESS
//     }
// }

// execute_login_to_google_cloud();

// module.exports = {
//     execute_login_to_google_cloud,
// };
