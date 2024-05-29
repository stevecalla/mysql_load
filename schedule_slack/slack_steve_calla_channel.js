// https://www.locale.ai/blog/automate-slack-messages-node-js
// https://ezhire.slack.com/services/B075P6JS9GB?added=1

// setup .env file
const dotenv = require('dotenv');
dotenv.config({path: "../.env"}); 

console.log(process.env.SLACK_WEBHOOK_STEVE_CALLA_CHANNEL_URL);

async function sendSlackMessage() {
    const message = `Today's USD to INR Exchange Rate: slack hook`;
    try {
        // const response = await fetch(SLACK_WEBHOOK_URL, {
        const response = await fetch(process.env.SLACK_WEBHOOK_STEVE_CALLA_CHANNEL_URL, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ 
            text: message,
            "icon_emoji": ":ghost:",
            "username": "new-bot-name",
        }),
        // "channel": "#development", // didn't test
        // "channel": "@Steve Calla", // didn't work
      });
      if (response.ok) {
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

main();
