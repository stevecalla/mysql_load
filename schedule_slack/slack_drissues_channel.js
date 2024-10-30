const dotenv = require('dotenv');
dotenv.config({ path: "../.env" });
const axios = require('axios');

async function sendSlackMessage(message = "test message") {
  const url = process.env.SLACK_WEBHOOK_DRISSUES_CHANNEL_URL;

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
      console.log('Message sent to eZhire Slack DR ISSUES channel');
    } else {
      throw new Error(`Error sending message to Slack: ${response.status} ${response.statusText}`);
    }
  } catch (error) {
    console.error('Error sending message to Slack:', error);
  }
}


async function slack_message_drissues_channel(fail_message) {
  await sendSlackMessage(fail_message);
}

// slack_message_drissues_channel('testing slack bot automated message');

module.exports = {
  slack_message_drissues_channel,
}
