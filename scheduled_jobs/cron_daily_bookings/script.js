console.log(`\nHello - RUN DAILY BOOKING JOB`);
console.log("Current Date and Time:", new Date().toLocaleString());

fetch('http://localhost:8000/hourlyReport')
    .then(response => {

        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        return response.text();  // Use response.json() if expecting JSON data
    })
    .then(data => {
        console.log('Response from hourlyReport:', data);
    })
    .catch(error => {
        console.error('Error with request:', error.message);
    });

    // C:\Users\calla\development\ezhire\programs\scheduled_jobs\cron_daily_bookings\script.js