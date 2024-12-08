const dayjs = require('dayjs');

//returns "2024-03-10_10-25-25"
function getCurrentDateTimeForFileNaming() {
    const createdAt = dayjs(); // Current date and time
    const createdAtFormatted = createdAt.format('YYYY-MM-DD_HH-mm-ss');
    // console.log('Current date and time = ', createdAtFormatted);
    return createdAtFormatted;
}

//returns "2024-03-10 10:25:25"
function getCurrentDateTime() {
    const createdAt = dayjs(); // Current date and time
    const createdAtFormatted = createdAt.format('YYYY-MM-DD HH:mm:ss');
    // console.log('Current date and time = ', createdAtFormatted);
    return createdAtFormatted;
}

//returns "10:25:25"
function getCurrentTime() {
    const createdAt = dayjs(); // Current date and time
    const createdAtFormatted = createdAt.format('HH:mm:ss');
    // console.log('Current date and time = ', createdAtFormatted);
    return createdAtFormatted;
}

//returns "2024-03-10"
function getCurrentDateForFileNaming() {
    const createdAt = dayjs(); // Current date and time
    const createdAtFormatted = createdAt.format('YYYY-MM-DD');
    // console.log('Current date = ', createdAtFormatted);
    return createdAtFormatted;
}

//takes 2024-07-01T06:00:00.000Z; returns "2024-03-10"
function getFormattedDate(date) {
    date = dayjs(date);
    const createdAtFormatted = date.format('YYYY-MM-DD');
    // console.log('Current date = ', createdAtFormatted);
    return createdAtFormatted;
}

//takes '2024-10-19 16:05:27'; returns '2024-10-19 04:05:27 pm'
function getFormattedDateAmPm(date) {
    date = dayjs(date);

    const createdAtFormatted = date.format('YYYY-MM-DD hh:mm:ss a');

    return createdAtFormatted;
}

// Function to convert Unix timestamp 1712179121648 to 2024-04-03 15:18:41
function convertTimestampToDateTime(timestamp) {
    // Ensure the timestamp is parsed as a number (if it's a string)
    const timestampNumber = typeof timestamp === 'string' ? parseInt(timestamp, 10) : timestamp;

    // Create a Day.js object from the Unix timestamp (in milliseconds)
    const dateObj = dayjs(timestampNumber);

    // Format the date object to yy mm dd hh mm ss format
    const formattedDateTime = dateObj.format('YYYY-MM-DD HH:mm:ss');

    // console.log(timestamp);
    // console.log(formattedDateTime);

    return formattedDateTime;
}

// Function to get the current date and convert to Pakistan Standard Time
async function getPakistanTime(date) {
    // Use current date if no date is provided
    if (!date) {
        date = new Date();
    }

    // Convert the current date to UTC time
    const utcDate = new Date(date.toISOString());

    // Adjust the time to UTC+5 (Pakistan Standard Time)
    const pakistanOffset = 5 * 60; // Offset in minutes
    let pakistanDate = new Date(utcDate.getTime() + pakistanOffset * 60 * 1000);

    const year = pakistanDate.getFullYear(); // Get the year
    const month = String(pakistanDate.getMonth() + 1).padStart(2, '0'); // Get the month (0-based, so add 1)
    const day = String(pakistanDate.getDate()).padStart(2, '0'); // Get the day of the month

    pakistanDate = `${year}-${month}-${day}`; // Combine into YYYY-MM-DD format

    return pakistanDate;
}

// getCurrentDateForFileNaming();
// getCurrentDateTimeForFileNaming();
// convertTimestampToDateTime('1712179121648');

module.exports = {
    getCurrentDateTimeForFileNaming,
    getCurrentDateForFileNaming,
    getCurrentDateTime,
    getCurrentTime,
    convertTimestampToDateTime,
    getFormattedDate,
    getFormattedDateAmPm,
    getPakistanTime,
}