// https://www.locale.ai/blog/automate-slack-messages-node-js
// https://ezhire.slack.com/services/B077HA1CCEB // = dr-issues channel integration settings
// https://ezhire.slack.com/apps/A0F7XDUAZ-incoming-webhooks?tab=settings&next_id=0

// setup .env file
const dotenv = require('dotenv');
dotenv.config({ path: "../.env" });
const axios = require('axios');

// console.log(process.env.SLACK_WEBHOOK_DRISSUES_CHANNEL_URL);

async function sendSlackMessage(message = "test message") {
  const slack_message = `${message}`;

  const url = process.env.SLACK_WEBHOOK_325_BOOKING_CHANNEL_URL;

  // Create the payload object inside the function
  const payload = {
    text: message,
    icon_emoji: ":ghost:",
    username: "Steve Calla",
  };

  // Check if fetch is available
  const isFetchAvailable = typeof fetch === 'function';

  try {
    let response;

    if (isFetchAvailable) {
      response = await fetch(url, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(payload),
      });
    } else {
      // Fallback to axios
      response = await axios.post(url, payload);
    }

    if (response.ok || response.status === 200) {
      console.log('Message sent to eZhire Slack 325 Bookings Channel');
    } else {
      throw new Error(`Error sending message to Slack: ${response.status} ${response.statusText || response.status}`);
    }
  } catch (error) {
    console.error('Error sending message to Slack:', error);
  } 
}

async function slack_message_325_bookings_channel(message) {
  await sendSlackMessage(message);
}

// slack_message_drissues_channel('testing slack bot automated message');

module.exports = {
  slack_message_325_bookings_channel,
}
