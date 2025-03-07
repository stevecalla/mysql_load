const { getFormattedDateAmPm } = require('../utilities/getCurrentDate');

// CREATE DATE INFO
async function date_info(data, effectiveDate) {
  // DATE INFO
  const query_date = `${getFormattedDateAmPm(data[0].queried_at_gst)} GST`;
  const queried_at_message = `Info Queried At: ${query_date}`;

  const most_recent_date = `${getFormattedDateAmPm(data[0].max_created_on_gst)} GST`;
  const most_recent_date_message = `Most Recent Lead At: ${most_recent_date}`;

  const booking_date = `${effectiveDate}`;
  const booking_date_message = `For Booking Date: ${booking_date}`;

  return { queried_at_message, most_recent_date_message, booking_date_message };
}

// CREATE LOOKER STUDIO LINKS
async function looker_links() {
  const link_dashboard = `https://lookerstudio.google.com/u/0/reporting/20953aff-a544-445b-91ba-2f2d378a70c3/page/p_zsb11me7nd?pli=1`;
  const looker_dashboard_link = `<${link_dashboard}|Link to Looker Leads Dashboard>`;
  
  const link_chart = `https://lookerstudio.google.com/u/0/reporting/20953aff-a544-445b-91ba-2f2d378a70c3/page/p_dirkgfi7nd?pli=1`;
  const looker_chart_link = `<${link_chart}|Link to Looker Leads Chart>`;

  return { looker_dashboard_link, looker_chart_link };
}

async function create_daily_lead_slack_message(data, tables) {
  const { effectiveDate, countryFilter, country_table_output, source_table_output, shift_table_output, response_time_table_output, response_time_by_shift_leads_output, response_time_by_shift_bookings_output, response_time_by_shift_conversion_output } = tables;

  const { queried_at_message, most_recent_date_message, booking_date_message } = await date_info(data, effectiveDate);

  console.log(`Using data for date: ${effectiveDate} and country: ${countryFilter || "All Countries"}`);

  // FINAL MESSAGE
  const slackMessage = 
    `\n**************\n` +
    `LEADS DATA\n` +
    `${queried_at_message}\n` +
    `${most_recent_date_message}\n` +
    `${booking_date_message}\n` +
    `Country: ${!countryFilter ? "All Countries" : countryFilter.toUpperCase()}\n` +

    `--------------\n` +

    `*By Country - ${effectiveDate}:* \n` + 
    `\`\`\`${country_table_output}\`\`\`` + `\n` + 

    `*By Source - ${effectiveDate}:* \n` + 
    `\`\`\`${source_table_output}\`\`\`` + `\n` +

    `*By Shift - ${effectiveDate}:* \n` + 
    `\`\`\`${shift_table_output}\`\`\`` + `\n` +

    `*By Response Time - ${effectiveDate}:* \n` + 
    `\`\`\`${response_time_table_output}\`\`\`` + `\n` +
    
    `\`\`\`All/Valid = Leads; Conf/Same = Bookings Confirmed/Same Day\n` +
    `% Conv = Conversion Ratio. % Conv based on valid leads\`\`\`` + `\n` + 

    `\`\`\`Differs from ERP report due to (a) duplicate elimination, (b) time zone\n` +
    `adjustment, (c) timing, and (d) booking rental status = cancel\`\`\`` + `\n` +      

    `**************\n`
  ;

  console.log(slackMessage);

  return slackMessage;
}

async function create_daily_lead_response_slack_message(data, tables) {
  const { effectiveDate, countryFilter, country_table_output, source_table_output, shift_table_output, response_time_table_output, response_time_by_shift_leads_output, response_time_by_shift_bookings_output, response_time_by_shift_conversion_output } = tables;

  const { queried_at_message, most_recent_date_message, booking_date_message } = await date_info(data, effectiveDate);

  const { looker_dashboard_link, looker_chart_link } = await looker_links();

  console.log(`Using data for date: ${effectiveDate} and country: ${countryFilter || "All Countries"}`);

  // FINAL MESSAGE
  const slackMessage = 
    `\n**************\n` +
    `LEADS - RESPONSE TIME BY SHIFT\n` +
    `${queried_at_message}\n` +
    `${most_recent_date_message}\n` +
    `${booking_date_message}\n` +
    `Country: ${!countryFilter ? "All Countries" : countryFilter.toUpperCase()}\n` +
    `--------------\n` +

    `*Response by Shift  - Leads:* \n` + 
    `\`\`\`${response_time_by_shift_leads_output}\`\`\`` + `\n` +

    `*Response by Shift  - Bookings:* \n` + 
    `\`\`\`${response_time_by_shift_bookings_output}\`\`\`` + `\n` +

    `*Response by Shift  - Conversion:* \n` + 
    `\`\`\`${response_time_by_shift_conversion_output}\`\`\`` + `\n` +
    
    `\`\`\`Differs from ERP report due to (a) duplicate elimination, (b) time zone\n` +
    `adjustment, (c) timing, and (d) booking rental status = cancel\`\`\`` + `\n` + 
    
    `${looker_dashboard_link}` + `\n` +
    `${looker_chart_link}` + `\n` +
    `**************\n`
  ;

  console.log(slackMessage);

  return slackMessage;
}

module.exports = {
  create_daily_lead_slack_message,
  create_daily_lead_response_slack_message,
}