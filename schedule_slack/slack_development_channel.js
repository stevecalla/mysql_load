// https://www.locale.ai/blog/automate-slack-messages-node-js
// https://ezhire.slack.com/services/B075P6JS9GB?added=1 = steve calla channel integration settings
// https://ezhire.slack.com/services/B077HA1CCEB // = development channel integration settings
// https://ezhire.slack.com/apps/A0F7XDUAZ-incoming-webhooks?tab=settings&next_id=0

// setup .env file
const dotenv = require('dotenv');
dotenv.config({path: "../.env"}); 

// console.log(process.env.SLACK_WEBHOOK_DEVELOPMENT_CHANNEL_URL);

async function sendSlackMessage(message) {
  const slack_message = `${message}`;

    try {
        const response = await fetch(process.env.SLACK_WEBHOOK_DEVELOPMENT_CHANNEL_URL, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ 
            text:slack_message,
            "icon_emoji": ":ghost:",
            "username": "Steve Calla",
        }),
      });
      if (response.ok) {
        console.log('Message sent to eZhire Slack Development channel');
      } else {
        throw new Error(`Error sending message to Slack: ${response.status} ${response.statusText}`);
      }
    } catch (error) {
      console.error('Error sending message to Slack:', error);
    }
  }
  
async function slack_message_development_channel(fail_message) {
    await sendSlackMessage(fail_message);
}

// slack_message_development_channel('testing');

module.exports = {
  slack_message_development_channel,
}
