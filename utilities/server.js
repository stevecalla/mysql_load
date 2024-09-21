const express = require('express');
const bodyParser = require('body-parser');

// SLACK SETUP
const { WebClient } = require('@slack/web-api');
const { execute_get_daily_booking_data } = require('../daily_booking_forecast/step_1_sql_get_daily_booking_data'); //step_1
// Initialize Slack Web API client
const slackClient = new WebClient(process.env.SLACK_BOT_TOKEN); // Make sure to set your token

const app = express();
const PORT = 8000; // You can change this port if needed

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
    await sendFollowUpMessage(req.body.channel_id, slackMessage);
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
});


// Function to send follow-up message to Slack
async function sendFollowUpMessage(channelId, message) {
    console.log(channelId, message);

    try {
        await slackClient.chat.postMessage({
            channel: channelId,
            text: message,
        });
        console.log('Message sent to Slack');
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
    const goal_message = `Goal: ${goal}`;
    let booking_count_message = `Bookings: ${bookings_today}`;
    let today_above_below_goal_message = `Above / Below Goal: ${today_above_below_goal}`;

    let pacing_thresholds = `Pacing Goals: 10a-11a = 60, noon-1p = 100, 4p-5p = 200, 8p-10p = 300`
    
    let today_status = today_above_below_goal >= goal ? '✅🚀 Well done!' : '💪 Keep going!';
    let yesterday_status = `Yesterday: ${bookings_yesterday}, ${yesterday_above_below_goal} ${yesterday_above_below_goal >= 0 ? ' more than goal. Well done!🔥🔥' : 'less than goal. We’re learning from this! 👀'}`;
    
    slackMessage = `\n**************\nUAE ONLY\n${created_at_date}\n${most_recent_booking_date}\n--------------\n${booking_count_message}\n${goal_message}\n${today_above_below_goal_message}\n${today_status}\n--------------\n${pacing_thresholds}\n--------------\n${yesterday_status}\n**************\n`;
    console.log(slackMessage);

    return slackMessage;
}

