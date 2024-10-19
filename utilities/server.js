const express = require('express');
const bodyParser = require('body-parser');
const axios = require('axios');

// SLACK SETUP
const localtunnel = require('localtunnel');
const ngrok = require('ngrok');
const { WebClient } = require('@slack/web-api');
const { execute_get_daily_booking_data } = require('../daily_booking_forecast/step_1_sql_get_daily_booking_data'); //step_1
// Initialize Slack Web API client
const slackClient = new WebClient(process.env.SLACK_BOT_TOKEN); // Make sure to set your token
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
    const getResults = await execute_get_daily_booking_data();
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

async function localTunnel(PORT1) {
    try {
        const tunnel1 = await localtunnel({ port: PORT1 });
        console.log(`Localtunnel for Server 1 is available at: ${tunnel1.url}`);
        
        tunnel1.on('close', () => {
            console.log('Localtunnel for Server 1 is closed');
        });
    } catch (error) {
        console.error(`Error starting localtunnel for Server 1: ${error.message}`);
    }
}

// Start the server
app.listen(PORT, () => {
    console.log(`Server is running on http://localhost:${PORT}`);

    startNgrok();
    // localTunnel(PORT);
});




