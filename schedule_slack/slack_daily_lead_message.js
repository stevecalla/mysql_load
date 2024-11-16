const { getFormattedDateAmPm } = require('../utilities/getCurrentDate');

// const { execute_get_daily_lead_data } = require('../daily_lead_setup/step_1_sql_get_daily_lead_data');

const { lead_data } = require('../daily_lead_setup/seed_data');
const { format_lead_data } = require('../daily_lead_setup/utility_group_data');


function generateLeadSummary(outputText, segment) {
  // Destructure the values from the output object
  const {
    yesterday_leads,
    today_leads,
    yesterday_booking_cancelled,
    yesterday_booking_confirmed,
    yesterday_booking_total,
    today_booking_cancelled,
    today_booking_confirmed,
    today_booking_total,
    yesterday_booking_conversion,
    today_booking_conversion,
  } = outputText;

  // Generate the lead summary based on the segment
  let leadSummary = '';
  
  if (segment === 'uae_country') {
    leadSummary = `LEADS - UAE ONLY\n` +
      `${yesterday_leads}, Confirmed Bookings ${yesterday_booking_confirmed}, Conversion ${yesterday_booking_conversion}\n` +
      `${today_leads}, Confirmed Bookings ${today_booking_confirmed}, Conversion ${today_booking_conversion}\n--------------`;
  } else if (segment === 'uae_source') {
    leadSummary = `SOURCE - UAE ONLY\n` +
      `${yesterday_leads}\n${yesterday_booking_conversion} (conversion)\n` +
      `${today_leads}\n${today_booking_conversion} (conversion)\n--------------`;
  } else if (segment === 'all_countries') {
    leadSummary = `LEADS - ALL COUNTRIES\n` +
      `${yesterday_leads}\n${yesterday_booking_conversion} (conversion)\n` +
      `${today_leads}\n${today_booking_conversion} (conversion)\nUNK: Unknown = country blank\n--------------`;
  } else if (segment === 'all_source') {
    leadSummary = `SOURCE - ALL COUNTRIES\n` +
      `${yesterday_leads}\n${yesterday_booking_conversion} (conversion)\n` +
      `${today_leads}\n${today_booking_conversion} (conversion)`;
  }

  return leadSummary;
}

async function create_daily_lead_slack_message(data) {
  const { all_countries_output_text, all_source_output_text, uae_only_country_output_text, uae_only_source_output_text } = await format_lead_data(data);

  const lead_uae_country = generateLeadSummary(uae_only_country_output_text, 'uae_country');
  const lead_uae_source = generateLeadSummary(uae_only_source_output_text, 'uae_source');
  const lead_all_countries = generateLeadSummary(all_countries_output_text, 'all_countries');
  const lead_all_source = generateLeadSummary(all_source_output_text, 'all_source');

  let { queried_at_message, most_recent_date_message } = await date_info(data);

  // FINAL MESSAGE
  // \n${most_recent_date_message} // took this out because the most recent lead at looks wrong
  const slackMessage = 
    `\n**************\n` +
    `LEADS DATA\n${queried_at_message}` +
    `\n--------------\n` +
    `${lead_uae_country}\n` +
    `${lead_uae_source}\n` +
    `${lead_all_countries}\n` +
    `${lead_all_source}\n` +
    `--------------\n` +
    `Response Time: IN PROGRESS\n` +
    `Conversion - Same Day: IN PROGRESS\n` +
    `**************\n`
  ;

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

// used to test
// async function main() {
//   // TEST WITH SEED DATA
//   create_daily_lead_slack_message(lead_data);

//   // TEST VIA THE API
//   // let data = await execute_get_daily_lead_data();

//   // create_daily_lead_slack_message(data);
// }

// main();

module.exports = {
  create_daily_lead_slack_message,
}