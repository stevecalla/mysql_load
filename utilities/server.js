const express = require('express');
const bodyParser = require('body-parser');
const axios = require('axios');

// SLACK SETUP
const ngrok = require('ngrok');
const { WebClient } = require('@slack/web-api');
const { execute_get_daily_booking_data } = require('../daily_booking_forecast/step_1_sql_get_daily_booking_data'); //step_1
// Initialize Slack Web API client
const slackClient = new WebClient(process.env.SLACK_BOT_TOKEN); // Make sure to set your token

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
    const processingMessage = "Retrieving booking information. Will respond shortly"; // Example data

    // Respond back to Slack
    res.json({
        text: processingMessage,
    });

    // Process the request asynchronously
    const getResults = await execute_get_daily_booking_data();
    const slackMessage = await createSlackMessage(getResults);
    console.log(slackMessage);

    // Send a follow-up message to Slack
    await sendFollowUpMessage(req.body.channel_id, req.body.channel_name, req.body.user_id, slackMessage);
});

// Endpoint to handle button click
// app.post('/button-click', (req, res) => {
//     // Parse the button click data
//     const payload = JSON.parse(req.body.payload);
    
//     // Fetch the latest stats (replace this with your actual logic)
//     const latestStats = "Latest bookings: 100"; // Example data

//     // Respond back to Slack
//     res.json({
//         text: latestStats,
//     });
// });

// Start the server
app.listen(PORT, () => {
    console.log(`Server is running on http://localhost:${PORT}`);

    startNgrok();
});

async function startNgrok() {
    try { 
        const ngrokUrl = await ngrok.connect(PORT);
        console.log(`Ngrok tunnel established at: ${ngrokUrl         }`);

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

async function createSlackMessage(results) {
    const goal = 325;
    const bookings_today = results[7].current_bookings;
    const bookings_yesterday = results[6].current_bookings;
    const today_above_below_goal = bookings_today - goal;
    const yesterday_above_below_goal = bookings_yesterday - goal;

    const created_at_date = `Info Updated At: ${results[0].created_at_gst} GST`;
    const most_recent_booking_date = `Most Recent Booking At: ${results[0].date_most_recent_created_on_gst} GST`;
    let booking_count_message = `📢Bookings: ${bookings_today}`;
    const goal_message = `🎯Goal: ${goal}`;
    let today_above_below_goal_message = `${today_above_below_goal >= 0 ? `🟢Goal: ${today_above_below_goal}` : `🔴Below Goal: ${today_above_below_goal}`}`;
    let pacing_thresholds = `Pacing Goals: 10a-11a = 60, noon-1p = 100, 4p-5p = 200, 8p-10p = 300`

    let today_status = today_above_below_goal >= goal ? '✅🚀Well done!' : '💪Keep going!';
    let yesterday_status = `Yesterday: ${yesterday_above_below_goal >= 0 ? `Well done🔥🔥 = ${bookings_yesterday}!! Bookings were +${yesterday_above_below_goal} above goal.` : `We’re learning from this! 👀. Bookings were ${yesterday_above_below_goal} below goal.`}`;

    const slackMessage = `\n**************\nUAE ONLY\n${created_at_date}\n${most_recent_booking_date}\n--------------\n${booking_count_message}\n${goal_message}\n${today_above_below_goal_message}\n${today_status}\n--------------\n${pacing_thresholds}\n--------------\n${yesterday_status}\n**************\n`;
    // console.log(slackMessage);

    return slackMessage;
}

