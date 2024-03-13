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

//returns "2024-03-10"
function getCurrentDateForFileNaming() {
    const createdAt = dayjs(); // Current date and time
    const createdAtFormatted = createdAt.format('YYYY-MM-DD');
    // console.log('Current date = ', createdAtFormatted);
    return createdAtFormatted;
}

// getCurrentDateForFileNaming();
// getCurrentDateTimeForFileNaming();

module.exports = {
    getCurrentDateTimeForFileNaming,
    getCurrentDateForFileNaming,
    getCurrentDateTime,
}