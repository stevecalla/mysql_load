const process = require("process");
const readline = require('readline');
const stdout = process.stdout;
const spinnerLibrary = require('./spinner.json');
const { getCurrentDateTime, getCurrentTime } = require('./getCurrentDate');

function getRandomNumber(max) {
  return Math.floor(Math.random() * max);
};

function getRandomSpinner() {
    // determine number of spinners
    const spinnerKeysLength = Object.keys(spinnerLibrary).length;
    // console.log(spinnerKeysLength);

    // list of available spinner keys
    let spinnerKeys = Object.keys(spinnerLibrary);
    // console.log(spinnerKeys);

    // get random spinner key
    let randomSpinnerKey = spinnerKeys[getRandomNumber(spinnerKeysLength)];
    // console.log(randomSpinnerKey);

    // get spinner array & interval
    let spinner = spinnerLibrary[randomSpinnerKey];
    
    // console.log(spinner);

    return spinner;
};

async function get_spinner(stop) {
    let index = 0;
    let count = 0;

    process.stdout.write("\x1B[?25l");

    let spinner = getRandomSpinner();
    
    const startTime = performance.now();

    let spinnerInterval = setInterval(() => {
        let line = spinner.frames[index];

        if (line === undefined) {
            index = 0;
            line = spinner.frames[index];
        };
        
        const endTime = performance.now();
        const elapsedTime = ((endTime - startTime) / 1_000).toFixed(0); //convert ms to sec

        readline.clearLine(process.stdout, 0);
        readline.cursorTo(process.stdout, 0);
        // let text = `waiting ... ${line}    Elapsed Time: ${elapsedTime} sec`;
        let text = `waiting ... ${line}    ${getCurrentTime()} Elapsed Time: ${elapsedTime} sec`;
        process.stdout.write(text);
        

        index = index >= spinner.length ? 0 : index + 1;
        count++;

        count > stop && clear_spinner(spinnerInterval);
    // }, spinner.interval);
    }, 1000);
};

function clear_spinner(spinnerName) {
    clearInterval(spinnerName);
}

get_spinner(20);

module.exports = {
    get_spinner,
    clear_spinner,
}

// RESOURCES:
// spinner idea: https://blog.bitsrc.io/build-command-line-spinners-in-node-js-3e432d926d56
// readline https://stackoverflow.com/questions/34570452/node-js-stdout-clearline-and-cursorto-functions