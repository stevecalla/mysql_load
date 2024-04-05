const { exec } = require('child_process');
const { generateLogFile } = require('../../utilities/generateLogFile');

const localFilePath = "\"C:/Users/calla/Google Drive/Resume & Stuff/ezhire/sql_analysis/programs/load_bigquery/example_test/example_test_csv_file.csv\"";
const bucketName = 'testing_bucket_v2';
const destinationPath = `gs://${bucketName}/`;

// Upload CSV file to Google Cloud Storage:
// gsutil cp path/to/local/my-csv-file.csv gs://my-bucket/

async function execute_load_data_to_cloud() {
  const startTime = performance.now();

  // Construct the command to upload the file using gsutil cp
  const command = `gsutil cp ${localFilePath} ${destinationPath}`;

  // Return a promise that resolves when the command execution is complete
  return new Promise((resolve, reject) => {
    exec(command, (error, stdout, stderr) => {
      if (error) {
        console.error('Error:', error);
        reject(error); // Reject the promise if there's an error
        return;
      }

      console.log('File uploaded successfully.');
      console.log('stdout:', stdout);
      console.error('stderr:', stderr);

      generateLogFile('load_cloud_data', `File uploaded successfully. ${stdout} ${stderr}`);

      const endTime = performance.now();
      const elapsedTime = ((endTime - startTime) / 1_000).toFixed(2); //convert ms to sec
      resolve(elapsedTime); // Resolve the promise with the elapsed time
    });
  });
}

module.exports = {
  execute_load_data_to_cloud,
}
