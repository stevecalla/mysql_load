console.log(`\nHello - RUN UPDATE LEADS JOB`);
console.log("Current Date and Time:", new Date().toLocaleString());

fetch('http://localhost:8000/update-forecast', {
    method: 'POST',
    headers: {
        'Content-Type': 'application/json',
    },
    body: JSON.stringify({}), // Send an empty JSON object as the payload
})
    .then(response => {
        if (response.ok) {
            console.log('Update forecast job initiated successfully.');
        } else {
            console.error(`Failed to initiate forecast job. HTTP status: ${response.status}`);
        }
    })
    .catch(error => {
        console.error('Error with request:', error.message);
    });
