console.log(`\nHello - RUN UPDATE LEADS JOB`);
console.log("Current Date and Time:", new Date().toLocaleString());

fetch('http://localhost:8000/update-leads', {
    method: 'POST', // Specify the HTTP method as POST
    headers: {
        'Content-Type': 'application/json', // Indicate the data format being sent
    },
    body: JSON.stringify({}), // Send an empty JSON object as the payload
})
    .then(response => {
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        return response.text(); // Use response.json() if expecting JSON response
    })
    .then(data => {
        console.log('Response from update-leads:', data);
    })
    .catch(error => {
        console.error('Error with request:', error.message);
    });
