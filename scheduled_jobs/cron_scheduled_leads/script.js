console.log(`\nHello - RUN SCHEDULED LEADS JOB`);
console.log("Current Date and Time:", new Date().toLocaleString());

fetch('http://localhost:8000/scheduled-leads')
    .then(response => {

        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        return response.text();  // Use response.json() if expecting JSON data
    })
    .then(data => {
        console.log('Response from scheduled-bookings:', data);
    })
    .catch(error => {
        console.error('Error with request:', error.message);
    });