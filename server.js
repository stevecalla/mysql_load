const express = require('express');
const bodyParser = require('body-parser');
const axios = require('axios');
const dotenv = require('dotenv');
dotenv.config();

// BOOKINGS SETUP
const { check_most_recent_created_on_date } = require('./get_most_recent_created_on/check_most_recent_created_on_date');
const { run_most_recent_check } = require('./scheduled_jobs/daily_bookings_by_hour.js');
const { execute_get_daily_booking_data } = require('./daily_booking_data/step_1_sql_get_daily_booking_data');
const { create_daily_booking_slack_message } = require('./schedule_slack/slack_daily_booking_message');

// CAR AVAILABILITY
const { execute_get_car_availability } = require('./daily_car_availability_data/step_1_sql_get_car_availability.js');

// FORECAST DATA
const { execute_get_booking_hourly_data } = require('./daily_booking_forecast/step_1_get_booking_by_hour_data.js');
const { execute_load_booking_by_hour_data } = require('./daily_booking_forecast/step_2_load_booking_by_hour_data.js');
const { execute_create_forecast_metrics } = require('./daily_booking_forecast/step_2a_create_forecast_summary_data.js');
const { execute_get_slack_forecast_data } = require('./daily_booking_forecast/step_3_get_slack_forecast_data');
const { execute_load_forecast_data_to_bigquery } = require('./load_bigquery/move_data_to_bigquery_forecast_data/step_0_load_main_job_040424.js');

// LEADS SETUP
const { execute_get_lead_response_data } = require('./daily_lead_response_setup/step_1_get_lead_response_data.js');
const { execute_load_lead_response_data } = require('./daily_lead_response_setup/step_2_load_lead_response_data.js');
const { execute_get_lead_metrics_data } = require('./daily_lead_response_setup/step_2a_create_lead_metrics_data.js');
const { execute_get_lead_data } = require('./daily_lead_response_setup/step_3_get_slack_lead_data.js');
const { execute_load_lead_data_to_bigquery } = require('./load_bigquery/move_data_to_bigquery_lead_data/step_0_load_main_job_040424.js');

// SLACK SETUP
const { WebClient } = require('@slack/web-api');
const slackClient = new WebClient(process.env.SLACK_BOT_TOKEN); // Make sure to set your token; Initialize Slack Web API client
const { slack_message_api } = require('./schedule_slack/slack_message_api.js');

// NGROK TUNNEL
const ngrok = require('ngrok');

// EXPRESS SERVER
const app = express();
const PORT = process.env.PORT || 8000; // You can change this port if needed

// Middleware
app.use(bodyParser.urlencoded({ extended: true }));
app.use(bodyParser.json());

// *********************************************
// BOOKING ROUTES - STARTT
// *********************************************
// Endpoint to handle crontab scheduled job
app.get('/scheduled-bookings', async (req, res) => {

    console.log('/scheduled-bookings route req.rawHeaders = ', req.rawHeaders);

    try {
        // Call the function to run the most recent check, retrieve data & send slack message
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

    const getBookingData = await execute_get_daily_booking_data(is_development_pool);
    const getCarAvailability = await execute_get_car_availability(is_development_pool);
    const getForecastData = await execute_get_slack_forecast_data(is_development_pool);

    const slackMessage = await create_daily_booking_slack_message(getBookingData, getCarAvailability, getForecastData);
    // console.log('slack message = ', slackMessage);

    // Send a follow-up message to Slack
    await sendFollowUpMessage(req.body.channel_id, req.body.channel_name, req.body.user_id, req.body.user_name, slackMessage);
});
// *********************************************
// BOOKING ROUTES - END
// *********************************************

// *********************************************
// FORECAST ROUTES - START
// *********************************************
async function update_forecast() {
    try {
        // STEP #0 USE DR OR PRODUCTION
        let is_testing = false; // test is_within_15_minutes in check_most_recent_created_on_date.js
        const result = await check_most_recent_created_on_date(is_testing); // USE DR DB OR PRODUCTION DB
        console.log('update forecast DR or production', result);
        const is_development_pool = result.is_development_pool; // use false to switch to production if necessary

        // STEP #1 GET FORECAST DATA FROM MYPROJECTS DR OR PRODUCTION
        const elapsed_time_get_data = await execute_get_booking_hourly_data(is_development_pool);

        // STEP #2 LOAD FORECAST DATA INTO LOCAL DB
        const elapsed_time_load_data = await execute_load_booking_by_hour_data();
      
        // STEP #3 CREATE FORECAST METRICS DATA
        const elapsed_time_create_metrics = await execute_create_forecast_metrics();

        // STEP #2B LOAD FORECAST & FORECAST METRICS DATA TO BIGQUERY
        const elapsed_time_load_data_to_biquery = await execute_load_forecast_data_to_bigquery();

        // STEP #3 SEND UPDATE MESSAGE TO S. CALLA
        const now = new Date().toLocaleString(); // Get the current local date and time as a string

        const slack_message = `🔔 Updated forecast data. Elapsed time to get data ${elapsed_time_get_data}. Elapsed time to load data ${elapsed_time_load_data}. Elapsed time to create metrics data ${elapsed_time_create_metrics}. Elapsed time to load lead data to biquery ${elapsed_time_load_data_to_biquery}. Time now = ${now} MTN.`;

        console.log('slack message after updating forecast data', slack_message);

        await slack_message_api(slack_message, 'steve_calla_slack_channel');

    } catch (error) {
        const error_message = `Error in update forecast route. ${error}.`;

        // STEP #3 SEND ERROR MESSAGE TO S. CALLA
        console.error(error_message);
        await slack_message_api(error_message, 'steve_calla_slack_channel');
    }
};

// Endpoint to handle "/update-forecast" command
app.post('/update-forecast', async (req, res, next) => {
    console.log('Update forecaast route received request to update forecast stats:', {
        body: req.body,
        headers: req.headers,
        param: req.params,  
    });

    try {
        res.status(202).json({ message: 'Update forecast job started successfully.' });
        await update_forecast(); 
    } catch (error) {
        console.log('Update forecast route error', error);
        // Send an error response
        res.status(500).json({
            message: 'Error updating forecast data.',
            error: error.message || 'Internal Server Error',
        });
    }
});

// *********************************************
// FORECAST ROUTES - END
// *********************************************

// *********************************************
// LEADS ROUTES - START
// *********************************************
// Store the last update timestamp in memory
let lastUpdateTimestamp = null;

async function check_last_leads_db_update() {
    // Used to ensure db is not updated to frequently. Time is set at 3 minute threshold
    const now = new Date().toLocaleString(); // Get the current local date and time as a string
    const currentTimestamp = new Date(); // Current timestamp as a Date object

    // Check if the route was updated in the last 10 minutes
    if (lastUpdateTimestamp) {
        const lastExecutionDate = new Date(lastUpdateTimestamp); // Convert the stored timestamp back to a Date object
        const timeDifference = (currentTimestamp - lastExecutionDate) / (1000 * 60); // Time difference in minutes

        if (timeDifference < 10) {
            const message = `🔔 Update-leads job was executed within the last 10 minutes at ${lastUpdateTimestamp}. Please try again in a couple of minutes. Time now = ${now} MTN.`;

            await slack_message_api(message, 'steve_calla_slack_channel');

            return true;
        }
    }
            
    // Update the timestamp
    lastUpdateTimestamp = now;
    return false;
};

async function update_leads() {
    try {
        // STEP #1 GET LEADS DATA FROM EZHIRE ERP DB
        const elapsed_time_get_data = await execute_get_lead_response_data(); // currently 2024 data

        // STEP #2 LOAD LEADS DATA INTO LOCAL DB
        const elapsed_time_load_data = await execute_load_lead_response_data();
      
        // STEP #3 CREATE LEAD METRICS DATA
        const elapsed_time_create_metrics = await execute_get_lead_metrics_data();

        // STEP #2B LOAD LEAD & LEAD METRICS DATA TO BIGQUERY
        const elapsed_time_load_lead_data_to_biquery = await execute_load_lead_data_to_bigquery();

        // STEP #3 SEND UPDATE MESSAGE TO S. CALLA
        const now = new Date().toLocaleString(); // Get the current local date and time as a string

        const slack_message = `🔔 Updated leads data. Elapsed time to get data ${elapsed_time_get_data}. Elapsed time to load data ${elapsed_time_load_data}. Elapsed time to create metrics data ${elapsed_time_create_metrics}. Elapsed time to load lead data to biquery ${elapsed_time_load_lead_data_to_biquery}. Time now = ${now} MTN.`;

        console.log('slack message after updating lead data', slack_message);

        await slack_message_api(slack_message, 'steve_calla_slack_channel');

    } catch (error) {
        const error_message = `Error in update leads route. ${error}.`;

        // STEP #3 SEND ERROR MESSAGE TO S. CALLA
        console.error(error_message);
        await slack_message_api(error_message, 'steve_calla_slack_channel');
    }
};

// Endpoint to handle "/update-leads" command
app.post('/update-leads', async (req, res, next) => {
    console.log('Update leads route received request to update stats:', {
        body: req.body,
        headers: req.headers,
        param: req.params,  
    });

    const is_within_last_ten_minutes = await check_last_leads_db_update();
    console.log('update-leads with last three minutes ', is_within_last_ten_minutes);

    if (is_within_last_ten_minutes) {
        res.status(202).json({ message: 'Update-leads within last three minutes. Try again soon.' });
        return;
    } else {
        res.status(202).json({ message: 'Update-leads job started successfully.' });
        await update_leads();
    }
});

// Function to handle sending the lead data as test or production
async function slack_send(message, test) {

    console.log('send to calla ', test);

    if (test) {
        await slack_message_api(message, "steve_calla_slack_channel");
        return;
    }

    await slack_message_api(message, "350_slack_channel");
    await slack_message_api(message, "400_slack_channel");
    await slack_message_api(message, "bilal_adhi_slack_channel");

    return;
};

// Endpoint to handle crontab scheduled job
app.get('/scheduled-leads/:country?/:date?', async (req, res) => {
    console.log('Received request for stats - /scheduled-leads route:', {
        body: req.body,
        headers: req.headers,
        param: req.params,
    });

    console.log('/scheduled-leads route req.rawHeaders = ', req.rawHeaders);

    // TESTING VARIABLES
    let send_slack_to_calla_test = false;

    let { country, date } = req.params;
    let countryFilter = '';

    // Default values for empty parameters
    if (country === "all") {
        country = '';
    } else {
        countryFilter = country?.trim() || '';
    }
    
    const dateFilter = date?.trim() || '';

    // Validate country format
    const valid_countries = ['bah', 'qat', 'sau', 'uae'];
    if (country && !valid_countries.includes(country.toLowerCase())) {
        const error_message = '⚠️ Invalid country. Please enter one of the following: BAH, QAT, SAU, or UAE.';
        send_slack_to_calla_test = true;
        await slack_send(error_message, send_slack_to_calla_test);
        return res.status(400).json({ message: error_message });
    }

    // Validate date format
    if (date && !/^\d{4}-\d{2}-\d{2}$/.test(date)) {
        const error_message = '⚠️ Invalid date format. Expected YYYY-MM-DD.';
        send_slack_to_calla_test = true;
        await slack_send(error_message, send_slack_to_calla_test);
        return res.status(400).json({ message: error_message });
    }

    try {
        // Await update to leads db if not within last 10 minutes
        const is_within_last_ten_minutes = await check_last_leads_db_update();
        !is_within_last_ten_minutes && await update_leads();

        // 1st parameter is count by first 3 characters or uae, 2nd parameter is date ie 2024-12-05
        const { no_data_message, slack_message_leads, slack_message_lead_response } = await execute_get_lead_data(countryFilter, dateFilter);

        if (no_data_message) {
            send_slack_to_calla_test = true;
            await slack_send(no_data_message, send_slack_to_calla_test);
        }

        if (slack_message_leads && slack_message_lead_response) {
            await slack_send(slack_message_leads, send_slack_to_calla_test);
            await slack_send(slack_message_lead_response, send_slack_to_calla_test);
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
app.post('/get-leads/:country?/:date?', async (req, res) => {

    console.log('Received request for stats - /get-leads route:', {
        body: req.body,
        headers: req.headers,
        param: req.params,
        text: req.body.text,
    });

    console.log('/get-leads route req.rawHeaders = ', req.rawHeaders);

    // Acknowledge the command from Slack immediately to avoid a timeout
    const processingMessage = "Retrieving lead information. Will respond shortly.";

    // Respond back to Slack
    res.json({
        text: processingMessage,
    });

    let countryFilter = '';
    let dateFilter = '';

    let channel_id = '';
    let channel_name = '';
    let user_id = '';
    let user_name = '';

    let text = '';
    let country = '';
    let date = '';

    if (req.body && Object.keys(req.body).length > 0) {

        channel_id = req.body.channel_id;
        channel_name = req.body.channel_name;
        user_id = req.body.user_id;
        user_name = req.body.user_name;
        text = req.body.text;

        if (text) {
            [country, date] = text.split(' '); // Split the text by spaces and destructure into variables

            console.log(`server js 266; Country: ${country}, Date: ${date}`);
        } else {
            console.log('Text is missing in the request body.');
        }

        // Default values for empty parameters
        if (country === "all") {
            country = '';
        } else {
            countryFilter = country?.trim() || '';
        }
        
        dateFilter = date?.trim() || '';
        console.log(`server js 279; Country: ${countryFilter}, Date: ${dateFilter}`);
        // Validate country format
        const valid_countries = ['bah', 'qat', 'sau', 'uae'];

        if (country && !valid_countries.includes(country.toLowerCase())) {
            const error_message = '⚠️ Invalid country. Please enter one of the following: BAH, QAT, SAU, or UAE.';

            // Send a follow-up message to Slack
            await sendFollowUpMessage(channel_id, channel_name, user_id, user_name, error_message);

            return;
        }

        // Validate date format
        if (date && !/^\d{4}-\d{2}-\d{2}$/.test(date)) {
            const error_message = '⚠️ Invalid date format. Expected YYYY-MM-DD.';

            // Send a follow-up message to Slack
            await sendFollowUpMessage(channel_id, channel_name, user_id, user_name, error_message);

            return;
        }
    }

    setTimeout( async () => {
        try {
            // 1st parameter is count by first 3 characters or uae, 2nd parameter is date ie 2024-12-05
            console.log(`server js 306; Country: ${countryFilter}, Date: ${dateFilter}`);

            // Await update to leads db if not within last 10 minutes
            const is_within_last_ten_minutes = await check_last_leads_db_update();
            !is_within_last_ten_minutes && await update_leads();

            // 1st parameter is count by first 3 characters or uae, 2nd parameter is date ie 2024-12-05
            const { no_data_message, slack_message_leads, slack_message_lead_response } = await execute_get_lead_data(countryFilter, dateFilter);

            if (no_data_message) {
                // Send a follow-up message to Slack
                await sendFollowUpMessage(channel_id, channel_name, user_id, user_name, no_data_message);
            }
    
            if (slack_message_leads && slack_message_lead_response) {
                // Send a follow-up message to Slack
                await sendFollowUpMessage(channel_id, channel_name, user_id, user_name, slack_message_leads);
                await sendFollowUpMessage(channel_id, channel_name, user_id, user_name, slack_message_lead_response);
            };
    
        } catch (error) {
            console.error('Error quering or sending leads:', error);
    
        }
    }, 3000);
});

// *********************************************
// LEADS ROUTES - END
// *********************************************

// Function to send follow-up message to Slack
async function sendFollowUpMessage(channelId, channelName, userId, userName, message) {
    try {
        if (channelId && message && channelName !== "directmessage") {
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
    console.log(`Tunnel using cloudflare https://ezhire.kidderwise.org/get-leads`)
    console.log(`http://192.168.1.220:8000`);

    // switched to cloudflare; see notes.txt

    // startNgrok();
});
