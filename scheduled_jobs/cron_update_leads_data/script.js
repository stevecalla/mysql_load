console.log(`\nHello - RUN UPDATE LEADS JOB`);
console.log("Current Date and Time:", new Date().toLocaleString());

fetch('http://localhost:8000/update-leads', {
    method: 'POST',
    headers: {
        'Content-Type': 'application/json',
    },
    body: JSON.stringify({}), // Send an empty JSON object as the payload
})
    .then(response => {
        if (response.ok) {
            console.log('Update-leads job initiated successfully.');
        } else {
            console.error(`Failed to initiate job. HTTP status: ${response.status}`);
        }
    })
    .catch(error => {
        console.error('Error with request:', error.message);
    });
