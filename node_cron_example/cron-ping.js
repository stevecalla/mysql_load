// const cron = require('node-cron');

// Schedule tasks to be run on the server.
// cron.schedule('* * * * * *', function() {
//     console.log('running a task every second');
//     console.log(new Date().toLocaleTimeString());
// });

// https://github.com/wahengchang/nodejs-cron-job-must-know
//https://github.com/wahengchang/nodejs-cron-job-must-know
//https://www.digitalocean.com/community/tutorials/nodejs-cron-jobs-by-examples
//https://stackabuse.com/executing-shell-commands-with-node-js/

const cron = require('node-cron');
const { exec } = require('child_process');

const commandList = [
    // 'cd ..',
    // 'node hello-cron.js',
    'node hello-cron.js',
];

// cron.schedule('*/1 * * * *', () => {
cron.schedule('* * * * * *', () => {
    console.log('Running cron job...');

    // Iterate through the command list and execute each command
    commandList.forEach((command) => {
        exec(command, (error, stdout, stderr) => {
            if (error) {
                console.error(`Error executing command: ${command}\n${error}`);
            } else {
                // const result = `${command}\n${stdout}`; // Assign the template string to a variable
                // console.log(`Command executed successfully: ${result}`);
            }
        });
    });

    console.log('Cron job completed.');
});

//   These asterisks are part of the crontab syntax to represent different units of time:
//   * * * * * *
//   | | | | | |
//   | | | | | day of week
//   | | | | month
//   | | | day of month
//   | | hour
//   | minute
//   second ( optional )
