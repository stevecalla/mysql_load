const { get_spinner, clear_spinner } = require('./spinner');

let max = 5;

async function testSpinner(max) {
    try {
        // Call the spinner function and wait for it to finish
        const test = await get_spinner(max);
        console.log(test);
    } catch (error) {
        console.error('Error occurred:', error);
    }
}

testSpinner(max);
