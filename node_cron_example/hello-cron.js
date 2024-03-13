// hello-cron.js
console.log('Hello from hello-cron.js!');

// The rest of your script...

const fs = require('fs');

function moveFilesToArchive() {
    console.log('Moving files to archive');

    try {
        // List all files in the directory
        const path = `C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/data/`;
        const files = fs.readdirSync(path);

        console.log(files);

        // Create the "archive" directory if it doesn't exist
        const testPath = `${path}test`;
        fs.mkdirSync((testPath), { recursive: true });

        // Iterate through each file
        for (const file of files) {
            if (file.endsWith('.csv')) {
                // Construct the full file paths
                const sourceFilePath = `${path}${file}`;
                const destinationFilePath = `${path}/test/${file}`;

                try {
                    // Move the file to the "archive" directory
                    fs.copyFileSync(sourceFilePath, destinationFilePath);
                    console.log(`Moved ${file}`);
                } catch (moveErr) {
                    console.error(`Error moving file ${file} to archive:`, moveErr);
                }
            }
        }

    } catch (readErr) {
        console.error('Error reading files:', readErr);
    }
}

moveFilesToArchive();