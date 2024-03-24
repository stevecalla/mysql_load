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

function get_spinner(stop) {
    let index = 0;
    let count = 0;

    process.stdout.write("\x1B[?25l");

    let spinner = getRandomSpinner();

    const startTime = performance.now();

    console.log(getCurrentDateTime());

    let spinnerInterval = setInterval(() => {
        const endTime = performance.now();
        const elapsedTime = ((endTime - startTime) / 1_000).toFixed(0); //convert ms to sec
        const runningTime = `Elapsed Time: ${elapsedTime} sec`;
        const message = `waiting ...`;

        readline.clearLine(process.stdout, 0);
        readline.cursorTo(process.stdout, 0);

        let renderSpinner = spinner.frames[index];

        if (renderSpinner === undefined) {
            index = 0;
            renderSpinner = spinner.frames[index];
        };

        let text = `${message} ${renderSpinner} ${runningTime}`;
        process.stdout.write(text);

        index = index >= spinner.length ? 0 : index + 1;
        count++;

        count > stop && clear_spinner(spinnerInterval);
    }, 500);

    // console.log(spinnerInterval);
    return spinnerInterval;
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