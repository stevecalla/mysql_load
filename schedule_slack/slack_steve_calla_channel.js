const dotenv = require('dotenv');
dotenv.config({ path: "../.env" });
const axios = require('axios');

async function sendSlackMessage(message) {
  const slack_message = `${message}`;

  const url = process.env.SLACK_WEBHOOK_STEVE_CALLA_CHANNEL_URL;

  const payload = {
    response_type: "ephemeral",  // Make the response visible only to the sender
    // response_type: "in_channel",  // Make the response visible to everyone
    text: slack_message,
    icon_emoji: ":ghost:",
    username: "Steve Calla",
  };

  // MIGHT NEED THIS TO SEND TABLE AS BLOCK BUT SEEMS TO BE WORKING FROM ABOVE
    // const payload = message;
    // let payload = {
    //   response_type: "in_channel",  // Make the response visible to everyone
    //   icon_emoji: ":ghost:",
    //   username: "Steve Calla",

    //   text: "Lead and Booking Data:",  // Fallback text for Slack clients that don't support blocks
    //   blocks: [
    //     {
    //       type: "section",
    //       text: {
    //         type: "mrkdwn",
    //         text: message
    //       }
    //     }
    //   ]
    // }

  try {
    let response;

    // Check if fetch is available
    if (typeof fetch !== 'undefined') {
      response = await fetch(url, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(payload),
      });


      if (!response.ok) {
        throw new Error(`Error sending message to Slack HELP: ${response.status} ${response.statusText}`);
      }

    } else {
      // Fallback to axios
      response = await axios.post(url, payload, {
        headers: {
          'Content-Type': 'application/json',
        },
      });
    }

    console.log('Message sent to eZhire Slack Steve Calla channel');
  } catch (error) {
    console.error('Error sending message to Slack:', error.response ? error.response.data : error.message);
  }
}

async function slack_message_steve_calla_channel(message) {
  await sendSlackMessage(message);
}

// slack_message_steve_calla_channel();

module.exports = {
  slack_message_steve_calla_channel,
}
