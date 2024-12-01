const { getFormattedDateAmPm } = require('../utilities/getCurrentDate');
const { lead_data } = require('../daily_lead_setup/seed_data_112924');
const { group_and_format_data_for_slack } = require('../daily_lead_setup/utility_group_and_format_data_for_slack');

function generateLeadSummary(outputText, segment) {
  // Destructure the values from the output object
  const {
    yesterday_leads_valid,
    today_leads_valid,
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
  const leadSummaryMap = {
    all: () => 
      `LEADS - ALL COUNTRIES\n` +
      `${today_leads_valid}, Bookings Confirmed - ${today_booking_confirmed}, Conversion - ${today_booking_conversion}\n` +
      `${yesterday_leads_valid}, Bookings Confirmed - ${yesterday_booking_confirmed}, Conversion - ${yesterday_booking_conversion}\n` +
      `--------------`,
    
    uae_country: () => 
      `LEADS - UAE ONLY\n` +
      `${today_leads_valid}, Bookings Confirmed - ${today_booking_confirmed}, Conversion - ${today_booking_conversion}\n` +
      `${yesterday_leads_valid}, Bookings Confirmed - ${yesterday_booking_confirmed}, Conversion - ${yesterday_booking_conversion}\n` +
      `--------------`,
    
    uae_source: () => 
      `SOURCE - UAE ONLY\n` +
      `${today_leads_valid}\n${today_booking_conversion} (conversion)\n` +
      `${yesterday_leads_valid}\n${yesterday_booking_conversion} (conversion)\n` +
      `--------------`,
    
    all_countries: () => 
      `LEADS - ALL COUNTRIES\n` +
      `${today_leads_valid}\n${today_booking_conversion} (conversion)\n` +
      `${yesterday_leads_valid}\n${yesterday_booking_conversion} (conversion)\n` +
      `UNK: Unknown = country blank\n` +
      `--------------`,
    
    all_source: () => 
      `SOURCE - ALL COUNTRIES\n` +
      `${yesterday_leads_valid}\n${yesterday_booking_conversion} (conversion)\n` +
      `${today_leads_valid}\n${today_booking_conversion} (conversion)`
  };
  
  // Use the map to get the lead summary
  return leadSummaryMap[segment]?.() || "Segment not recognized.";
  
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

async function create_daily_lead_slack_message(data) {
  // TEXT OUTPUT
  const { only_all_countries_output_text, all_countries_output_text, all_source_output_text, uae_only_country_output_text, uae_only_source_output_text, table_output_by_country, table_output_by_source } = await group_and_format_data_for_slack(data);

  const all_summary = generateLeadSummary(only_all_countries_output_text, 'all');
  const lead_uae_country = generateLeadSummary(uae_only_country_output_text, 'uae_country');

  // const lead_uae_source = generateLeadSummary(uae_only_source_output_text, 'uae_source');
  // const lead_all_countries = generateLeadSummary(all_countries_output_text, 'all_countries');
  // const lead_all_source = generateLeadSummary(all_source_output_text, 'all_source');

  // TABLES OUTPUT
  
  const { today_table_by_segment: today_table_by_country, yesterday_table_by_segment: yesterday_table_by_country } = table_output_by_country;
  const { today_table_by_segment: today_table_by_source, yesterday_table_by_segment: yesterday_table_by_source } = table_output_by_source;

  let { queried_at_message, most_recent_date_message } = await date_info(data);

  // FINAL MESSAGE
  const slackMessage = 
    `\n**************\n` +
    `LEADS DATA\n` +
    `${queried_at_message}\n` +
    `${most_recent_date_message}\n` + // took this out because the most recent lead at looks wrong
    `--------------\n` +
    `${lead_uae_country}\n` +
    `${all_summary}\n` +
    // `${lead_uae_source}\n` +
    // `${lead_all_countries}\n` +
    // `${lead_all_source}\n` + 

    "*Today - By Country:* \n" + 
    `\`\`\`${today_table_by_country}\nAll/Valid = Leads; Conf/Same = Bookings Confirmed/Same Day\n` +
    `% Conv = Conversion Ratio. % Conv based on valid leads\`\`\`` + `\n` + 

    // "*Yesterday - By Country:* \n" + 
    // `\`\`\`${yesterday_table_by_country}\`\`\`` + `\n`+

    "*Today - By Source:* \n" +
    `\`\`\`${today_table_by_source}\`\`\`` + `\n` +

    `\`\`\`Differs from ERP report due to (a) duplicate elimination, (b) time zone...\n` +
    `... adjustment, (c) timing, and (d) booking rental status = cancel\`\`\`` + `\n` +      
    `**************\n`
    `Response Time: IN PROGRESS\n` +
    `**************\n`
  ;

  console.log(slackMessage);

  return slackMessage;
}

// TESTING FUNCTION WITH SEED DATA
async function testing() {
  // let slack_message = await create_daily_lead_slack_message(lead_data);
  
  // console.log(slack_message);
  
  // const { slack_message_steve_calla_channel } = require('./slack_steve_calla_channel');
  // await slack_message_steve_calla_channel(slack_message);
  
  // TEST VIA THE API
  const { execute_get_daily_lead_data } = require('../daily_lead_setup/step_1_sql_get_daily_lead_data');
  let data = await execute_get_daily_lead_data();

  let slack_message = await create_daily_lead_slack_message(data);

  // console.log(slack_message);
  
  const { slack_message_steve_calla_channel } = require('./slack_steve_calla_channel');
  const { slack_message_bilal_adhi_channel } = require('./slack_bilal_adhi_channel');

  await slack_message_steve_calla_channel(slack_message);
  await slack_message_bilal_adhi_channel(slack_message);
}

testing();

module.exports = {
  create_daily_lead_slack_message,
}