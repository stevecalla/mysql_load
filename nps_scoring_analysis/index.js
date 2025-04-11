// index.js
const { exec } = require("child_process");

exec("python nps_scores_analysis.py", (error, stdout, stderr) => {
    if (error) {
        console.error(`Error: ${error.message}`);
        return;
    }
    if (stderr) {
        console.error(`stderr: ${stderr}`);
        return;
    }
    console.log(stdout);
    // console.log(`Python Output: ${stdout}`);
});