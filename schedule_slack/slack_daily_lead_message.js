const { getFormattedDateAmPm } = require('../utilities/getCurrentDate');

const { execute_get_daily_lead_data } = require('../daily_lead_setup/step_1_sql_get_daily_lead_data');
const { format_lead_data } = require('../daily_lead_setup/utility_group_data');
const { all } = require('axios');

async function create_daily_lead_slack_message(data) {
  const { leads_rollup_by_country, all_countries_output_text, uae_only_output_text, source_output_text, uae_only_source_output_text } = await format_lead_data(data);

  const { 
      yesterday,
      today,
  } = all_countries_output_text;

  const {
    yesterday: yesterday_uae,
    today: today_uae
  } = uae_only_output_text;

  const {
    yesterday: yesterday_source,
    today: today_source
  } = source_output_text

  const {
    yesterday: yesterday_uae_source,
    today: today_uae_source
  } = uae_only_source_output_text

    let { queried_at_message, most_recent_date_message } = await date_info(data);

    // FINAL MESSAGE
    // \n${most_recent_date_message} // took this out because the most recent lead at looks wrong
    const slackMessage = 
      `\n**************\nLEADS DATA\n${queried_at_message}\n--------------\nLEADS - UAE ONLY\n${yesterday_uae}\n${today_uae}\n--------------\nSOURCE - UAE ONLY\n${yesterday_uae_source}\n${today_uae_source}\n--------------\nLEADS - All COUNTRIES\n${yesterday}\n${today}\nUNK: Unknown = country blank\n--------------\nSOURCE - ALL COUNTRIES\n${yesterday_source}\n${today_source}\n--------------\nResponse Time: TBD\nConversion: TBD\nConversion - Same Day: TBD\n**************\n`;

    console.log(slackMessage);

    return slackMessage;
}

// CREATE DATE INFO
async function date_info(data) {
  // DATE INFO
  const query_date = `${getFormattedDateAmPm(data[0].queried_at_gst)} GST`;
  const queried_at_message = `Info Queried At: ${query_date}`;

  const most_recent_date = `${getFormattedDateAmPm(data[0].max_created_on_gst)} GST`;
  const most_recent_date_message = `Most Recent Lead At: ${most_recent_date}`;

  return { queried_at_message, most_recent_date_message };
}

// // used to test
// async function main() {
//   let data = await execute_get_daily_lead_data();

//   create_daily_lead_slack_message(data);
// }

// main();

module.exports = {
  create_daily_lead_slack_message,
}