const express = require('express');
const bodyParser = require('body-parser');
const axios = require('axios');

// SLACK SETUP
const ngrok = require('ngrok');
const { WebClient } = require('@slack/web-api');
const { execute_get_daily_booking_data } = require('../daily_booking_forecast/step_1_sql_get_daily_booking_data'); //step_1

// Initialize Slack Web API client
const slackClient = new WebClient(process.env.SLACK_BOT_TOKEN); // Make sure to set your token

const { check_most_recent_created_on_date } = require('../get_most_recent_created_on/check_most_recent_created_on_date');
const { create_daily_booking_slack_message } = require('../schedule_slack/slack_daily_booking_message');

const app = express();
const PORT = process.env.PORT || 8000; // You can change this port if needed

// Middleware
app.use(bodyParser.urlencoded({ extended: true }));
app.use(bodyParser.json());

// Endpoint to handle slash command
app.post('/getstats', async (req, res) => {
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

    const getResults = await execute_get_daily_booking_data(is_development_pool);
    const slackMessage = await create_daily_booking_slack_message(getResults);
    // console.log(slackMessage);

    // Send a follow-up message to Slack
    await sendFollowUpMessage(req.body.channel_id, req.body.channel_name, req.body.user_id, slackMessage);
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




