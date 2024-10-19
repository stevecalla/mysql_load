// https://www.locale.ai/blog/automate-slack-messages-node-js
// https://ezhire.slack.com/services/B075P6JS9GB?added=1
// https://ezhire.slack.com/services/B0762026MFB // = development channel integration settings
// https://ezhire.slack.com/apps/A0F7XDUAZ-incoming-webhooks?tab=settings&next_id=0

// setup .env file
const dotenv = require('dotenv');
dotenv.config({path: "../.env"}); 
const axios = require('axios');

// console.log(process.env.SLACK_WEBHOOK_STEVE_CALLA_CHANNEL_URL);

async function sendSlackMessage() {
  const message = `Test slack development channel integration by Steve Calla.`;
  const url = process.env.SLACK_WEBHOOK_DEVELOPMENT_CHANNEL_URL;

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
      console.log('Exchange rate update sent to Slack');
    } else {
      throw new Error(`Error sending message to Slack: ${response.status} ${response.statusText}`);
    }
  } catch (error) {
    console.error('Error sending message to Slack:', error);
  }
}

async function main() {
  await sendSlackMessage();
}

  
async function main() {
    await sendSlackMessage();
}

main();
