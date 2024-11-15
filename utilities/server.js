const express = require('express');
const bodyParser = require('body-parser');
const axios = require('axios');

// BOOKINGS SETUP
const { check_most_recent_created_on_date } = require('../get_most_recent_created_on/check_most_recent_created_on_date');
const { run_most_recent_check } = require('../scheduled_jobs/daily_bookings_by_hour.js');
const { execute_get_daily_booking_data } = require('../daily_booking_forecast/step_1_sql_get_daily_booking_data');
const { create_daily_booking_slack_message } = require('../schedule_slack/slack_daily_booking_message');

// LEADS SETUP
const { execute_get_daily_lead_data } = require('../daily_lead_setup/step_1_sql_get_daily_lead_data.js');
const { create_daily_lead_slack_message } = require('../schedule_slack/slack_daily_lead_message');

// SLACK SETUP
const { WebClient } = require('@slack/web-api');
const slackClient = new WebClient(process.env.SLACK_BOT_TOKEN); // Make sure to set your token; Initialize Slack Web API client
const ngrok = require('ngrok');

// EXPRESS SERVER
const app = express();
const PORT = process.env.PORT || 8000; // You can change this port if needed

// Middleware
app.use(bodyParser.urlencoded({ extended: true }));
app.use(bodyParser.json());

// Endpoint to handle slash "bookings" command
app.post('/get-bookings', async (req, res) => {
    console.log('Received request for stats:', {
        body: req.body,
        headers: req.headers,
    });

    // Acknowledge the command from Slack immediately to avoid a timeout
    const processingMessage = "Retrieving booking information. Will respond shortly."; // Example data

    // Respond back to Slack
    res.json({
        text: processingMessage,
    });

    // Process the request asynchronously
    let is_testing = false; // used to test is_development_pool; normal state is false
    const result = await check_most_recent_created_on_date(is_testing); // USE DR DB OR PRODUCTION DB
    let is_development_pool = result.is_development_pool;
    // console.log(is_development_pool);

    // is_development_pool = false; // switch to production if necessary

    const getResults = await execute_get_daily_booking_data(is_development_pool);
    const slackMessage = await create_daily_booking_slack_message(getResults);
    // console.log(slackMessage);

    // Send a follow-up message to Slack
    await sendFollowUpMessage(req.body.channel_id, req.body.channel_name, req.body.user_id, slackMessage);
});

// Endpoint to handle slash "/leads" command
app.post('/get-leads', async (req, res) => {
    console.log('Received request for stats:', {
        body: req.body,
        headers: req.headers,
    });

    // Acknowledge the command from Slack immediately to avoid a timeout
    const processingMessage = "Retrieving leads information. Will respond shortly."; // Example data

    // Respond back to Slack
    res.json({
        text: processingMessage,
    });

    const getResults = await execute_get_daily_lead_data(); //fix change to leads
    const slackMessage = await create_daily_lead_slack_message(getResults); //fix change to leads
    // console.log(slackMessage);

    // Send a follow-up message to Slack
    await sendFollowUpMessage(req.body.channel_id, req.body.channel_name, req.body.user_id, slackMessage);
});

app.get('/hourlyReport', async (req, res) => {
    try {
        // Call the function to run the most recent check
        await run_most_recent_check();
        
        // Send a success response
        res.status(200).json({
            message: 'Hourly report check completed successfully.',
        });
    } catch (error) {
        console.error('Error running most recent check:', error);
        
        // Send an error response
        res.status(500).json({
            message: 'An error occurred while running the hourly report check.',
            error: error.message || 'Internal Server Error',
        });
    }
});

// Function to send follow-up message to Slack
async function sendFollowUpMessage(channelId, channelName, userId, message) {
    try {
        if(channelId && message && channelName !== "directmessage"){
            await slackClient.chat.postEphemeral({
                channel: channelId,
                user: userId,
                text: message,
            });
            console.log('Message sent to Slack');
        } else if (channelId && message && channelName === "directmessage") {
            await slackClient.chat.postMessage({
                channel: userId,
                text: message,
            });
            console.log('Message sent to Slack');
        } else {
            console.error('Channel ID or message is missing');
        }
    } catch (error) {
        console.error('Error sending message to Slack:', error);
    }
}

async function startNgrok() {
    try { 
        const ngrokUrl = await ngrok.connect(PORT);
        console.log(`Ngrok tunnel established at: ${ngrokUrl}`);

        // Fetch tunnel details from the ngrok API
        const apiUrl = 'http://127.0.0.1:4040/api/tunnels';
        const response = await axios.get(apiUrl);
        
        // Log tunnel information
        response.data.tunnels.forEach(tunnel => {
            // console.log({tunnel});
            console.log(`Tunnel: ${tunnel.public_url}`);
            console.log(`Forwarding to: ${tunnel.config.addr}`);
            console.log(`Traffic Inspector: https://dashboard.ngrok.com/ac_2J6Qn9CeVqC2bGd0EhZnAT612RQ/observability/traffic-inspector`)
            console.log(`Status: http://127.0.0.1:4040/status`)
        });

    } catch (error) {
        console.error(`Could not create ngrok tunnel: ${error}`);
    }
}

// Start the server
app.listen(PORT, () => {
    console.log(`Server is running on http://localhost:${PORT}`);

    startNgrok();
});




