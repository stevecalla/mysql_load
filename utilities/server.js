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

const { execute_get_lead_response_data } = require('../daily_lead_response_setup/step_1_get_lead_response_data.js');
const { execute_load_lead_response_data } = require('../daily_lead_response_setup/step_2_load_lead_response_data.js');
const { execute_get_lead_data } = require('../daily_lead_response_setup/step_3_get_slack_lead_data.js');

// SLACK SETUP
const { WebClient } = require('@slack/web-api');
const slackClient = new WebClient(process.env.SLACK_BOT_TOKEN); // Make sure to set your token; Initialize Slack Web API client
// const { slack_message_steve_calla_channel } = require('../schedule_slack/slack_steve_calla_channel.js');
// const { slack_message_400_bookings_channel } = require('../schedule_slack/slack_400_bookings_channel');
// const { slack_message_350_bookings_channel } = require('../schedule_slack/slack_350_bookings_channel');
// const { slack_message_bilal_adhi_channel} = require('../schedule_slack/slack_bilal_adhi_channel');

// NGROK TUNNEL
const ngrok = require('ngrok');
const { slack_message_api } = require('../schedule_slack/slack_message_api.js');

// EXPRESS SERVER
const app = express();
const PORT = process.env.PORT || 8000; // You can change this port if needed

// Middleware
app.use(bodyParser.urlencoded({ extended: true }));
app.use(bodyParser.json());

// Endpoint to handle crontab scheduled job
app.get('/scheduled-bookings', async (req, res) => {

    console.log('/scheduled-bookings route req.rawHeaders = ', req.rawHeaders);

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

// Endpoint to handle slash "/bookings" command
app.post('/get-bookings', async (req, res) => {
    console.log('Received request for stats:', {
        body: req.body,
        headers: req.headers,
    });
    console.log('/get-bookings route req.rawHeaders = ', req.rawHeaders);

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
    // console.log('slack message = ', slackMessage);

    // Send a follow-up message to Slack
    await sendFollowUpMessage(req.body.channel_id, req.body.channel_name, req.body.user_id, req.body.user_name, slackMessage);
});

// Endpoint to handle "/update-leads" command
app.post('/update-leads/:interval', async (req, res) => {
    console.log('Update leads route received request to update stats:', {
        body: req.body,
        headers: req.headers,
        param: req.params,
    });

    try {
    
        const date_interval = req.params.interval || 3; // retrieves most recent 4 days of data

        // STEP #1 GET LEADS DATA FROM EZHIRE ERP DB
        const elapsed_time_get_data = await execute_get_lead_response_data(date_interval);

        // STEP #2 LOAD LEADS DATA INTO LOCAL DB
        const elapsed_time_load_data = await execute_load_lead_response_data();

        // STEP #3 SEND UPDATE MESSAGE TO S. CALLA
        const slack_message = `Updated leads data with date interval of 3. Elapsed time to get data ${elapsed_time_get_data}. Elapsed time to load data ${elapsed_time_load_data}. Time now = ${new Date().toISOString()}`;

        await slack_message_api(slack_message, 'steve_calla_slack_channel');

        // Acknowledge the command from Slack immediately to avoid a timeout
        const processingMessage = `Successfully processed leads data update.`

        res.status(200).json({
            text: processingMessage,
        });

    } catch (error) {
        const error_message = `Error in execute_get_lead_response_data. ${error}.`

        // STEP #3 SEND UPDATE MESSAGE TO S. CALLA
        console.error(error_message);
        await slack_message_api(error_message, 'steve_calla_slack_channel');

        res.status(500).json({
            text: `Failed to update lead data. Check server logs for details.`,
        });
    } 

});

// Endpoint to handle crontab scheduled job
app.get('/scheduled-leads', async (req, res) => {
    console.log('Received request for stats - /scheduled-leads route:', {
        body: req.body,
        headers: req.headers,
        param: req.params,
    });
    
    console.log('/scheduled-leads route req.rawHeaders = ', req.rawHeaders);
    
    // TESTING VARIABLES
    let send_slack_to_calla = true;

    try {
        // const getResults = await execute_get_daily_lead_data();
        const { slack_message_leads, slack_message_lead_response } = await execute_get_lead_data();

        if (slack_message_leads && slack_message_lead_response) {

            if (send_slack_to_calla) {
                await slack_message_api(slack_message_leads, "steve_calla_slack_channel");
                await slack_message_api(slack_message_lead_response, "steve_calla_slack_channel");

              } else {
                await slack_message_400_bookings_channel(slack_message);
                await slack_message_350_bookings_channel(slack_message);
                await slack_message_bilal_adhi_channel(slack_message);
              }
        };
        
        // Send a success response
        res.status(200).json({
            message: 'Leads queried & sent successfully.',
        });
    } catch (error) {
        console.error('Error quering or sending leads:', error);
        
        // Send an error response
        res.status(500).json({
            message: 'Error quering or sending leads.',
            error: error.message || 'Internal Server Error',
        });
    }
});

// Endpoint to handle slash "/leads" command
app.post('/get-leads', async (req, res) => {
    console.log('Received request for stats:', {
        body: req.body,
        headers: req.headers,
    });
    console.log('/get-leads route req.rawHeaders = ', req.rawHeaders);

    // Acknowledge the command from Slack immediately to avoid a timeout
    const processingMessage = "Retrieving lead information. Will respond shortly."; // Example data

    // Respond back to Slack
    res.json({
        text: processingMessage,
    });

    const getResults = await execute_get_daily_lead_data();
    const slackMessage = await create_daily_lead_slack_message(getResults); 
    // console.log(slackMessage);

    // Send a follow-up message to Slack
    await sendFollowUpMessage(req.body.channel_id, req.body.channel_name, req.body.user_id, req.body.user_name, slackMessage);
});

// Function to send follow-up message to Slack
async function sendFollowUpMessage(channelId, channelName, userId, userName, message) {
    try {
        if(channelId && message && channelName !== "directmessage"){
            await slackClient.chat.postEphemeral({
                channel: channelId,
                user: userId,
                text: message,
            });
            console.log(`Message sent to Slack (channel name = ${channelName})`);
        } else if (channelId && message && channelName === "directmessage") {
            await slackClient.chat.postMessage({
                channel: userId,
                text: message,
            });
            console.log(`Message sent to Slack (channel name = ${channelName}; user name = ${userName})`);
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

// Clean up on exit
async function cleanup() {
    console.log('\nGracefully shutting down...');

    process.exit();
}

// Handle termination signals
process.on('SIGINT', cleanup);
process.on('SIGTERM', cleanup);

// Start the server
app.listen(PORT, () => {
    console.log(`Server is running on http://localhost:${PORT}`);

    console.log(`Tunnel using cloudflare https://ezhire.kidderwise.org/get-bookings`)
    console.log(`Tunnel using cloudflare https://usat-sales.kidderwise.org/get-leads`)
    console.log(`http://192.168.1.220:8000`);

    // switched to cloudflare; see notes.txt

    // startNgrok();
});




