const fs = require('fs');
// const jsonData = require('../load_bigquery/us-states.json');

const jsonData = `[
  {"name": "Alabama", "post_abbr": "AL"},
  {"name": "Alaska", "post_abbr":  "AK"},
  {"name": "Arizona", "post_abbr": "AZ"},
  {"name": "Arkansas", "post_abbr": "AR"},
  {"name": "California", "post_abbr": "CA"},
  {"name": "Colorado", "post_abbr": "CO"},
  {"name": "Connecticut", "post_abbr": "CT"},
  {"name": "Delaware", "post_abbr": "DE"},
  {"name": "Florida", "post_abbr": "FL"},
  {"name": "Georgia", "post_abbr": "GA"}
]`;

async function execute_create_csv_file() {
  
  const startTime = performance.now();

  // Parse the JSON data into an array of objects
  const parsedData = JSON.parse(jsonData);
  
  // Map each object to a CSV row string
  // const csvRows = data.map(obj => `${obj.name},${obj.post_abbr}`);
  
  // Get the keys from the first object
  const keys = Object.keys(parsedData[0]);
  
  // Create the necessary map dynamically
  const keyMap = keys.join(',');
  
  // Map the data to CSV rows using the dynamic map
  const csvRows = parsedData.map(obj => keys.map(key => obj[key]).join(','));
  
  // Combine CSV row strings into a single CSV string
  const csvString = csvRows.join('\n');
  
  // Save the CSV string to a file
  fs.writeFileSync('example_test_csv_file.csv', csvString);
  
  console.log('CSV file saved successfully.');
  
  const endTime = performance.now();
  const elapsedTime = ((endTime - startTime) / 1_000).toFixed(2); //convert ms to sec
  return elapsedTime;
}

module.exports = {
  execute_create_csv_file,
}
