const { getFormattedDateAmPm } = require('../utilities/getCurrentDate');

// const { execute_get_daily_lead_data } = require('../daily_lead_setup/step_1_sql_get_daily_lead_data');

const { lead_data } = require('../daily_lead_setup/seed_data');
const { format_lead_data } = require('../daily_lead_setup/utility_group_data');

async function create_daily_lead_slack_message(data) {
  const { all_countries_output_text, all_source_output_text, uae_only_country_output_text, uae_only_source_output_text } = await format_lead_data(data);

  // AVAILABLE FIELDS
  // yesterday_leads,
  // today_leads,
  // yesterday_booking_cancelled,
  // yesterday_booking_confirmed,
  // yesterday_booking_total,
  // today_booking_cancelled,
  // today_booking_confirmed,
  // today_booking_total,
  // yesterday_booking_conversion,
  // today_booking_conversion,

  const { 
      yesterday_leads,
      today_leads,
      today_booking_conversion: today_conversion_all_countries,
  } = all_countries_output_text;
  
  const {
    yesterday_leads: yesterday_leads_source,
    today_leads: today_leads_source,
    today_booking_conversion: today_conversion_all_source,
  } = all_source_output_text

  const {
    yesterday_leads: yesterday_leads_uae_country,
    today_leads: today_leads_uae_country,
    yesterday_booking_confirmed: yesterday_bookings_confirmed_uae_only,
    today_booking_confirmed: today_bookings_confirmed_uae_only,
    yesterday_booking_conversion: yesterday_conversion_uae_only,
    today_booking_conversion: today_conversion_uae_only

  } = uae_only_country_output_text;

  const {
    yesterday_leads: yesterday_leads_uae_source,
    today_leads: today_leads_uae_source,
    today_booking_conversion: today_conversion_uae_only_source,
  } = uae_only_source_output_text

    let { queried_at_message, most_recent_date_message } = await date_info(data);

    // FINAL MESSAGE
    // \n${most_recent_date_message} // took this out because the most recent lead at looks wrong
    const slackMessage = 
      `\n**************\nLEADS DATA\n${queried_at_message}\n--------------\nLEADS - UAE ONLY\n${yesterday_leads_uae_country}, Confirmed Bookings ${yesterday_bookings_confirmed_uae_only}, Conversion ${yesterday_conversion_uae_only}\n${today_leads_uae_country}, Confirmed Bookings ${today_bookings_confirmed_uae_only}, Conversion ${today_conversion_uae_only}\n--------------\nSOURCE - UAE ONLY\n${yesterday_leads_uae_source}\n${today_leads_uae_source}\n${today_conversion_uae_only_source} (conversion)\n--------------\nLEADS - All COUNTRIES\n${yesterday_leads}\n${today_leads}\n${today_conversion_all_countries} (conversion)\nUNK: Unknown = country blank\n--------------\nSOURCE - ALL COUNTRIES\n${yesterday_leads_source}\n${today_leads_source}\n${today_conversion_all_source} (conversion)\n--------------\nResponse Time: TBD\nConversion - Same Day: TBD\n**************\n`;

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
async function main() {
  // TEST WITH SEED DATA
  create_daily_lead_slack_message(lead_data);

  // TEST VIA THE API
  // let data = await execute_get_daily_lead_data();

  // create_daily_lead_slack_message(data);
}

main();

module.exports = {
  create_daily_lead_slack_message,
}