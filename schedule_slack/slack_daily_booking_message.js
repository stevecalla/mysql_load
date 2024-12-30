const { getFormattedDateAmPm } = require('../utilities/getCurrentDate');
const { get_country_data } = require('../daily_booking_data/results_grouped_by_country_by_cancel');
const { get_formatted_car_avail_data } = require('../daily_car_availability_data/format_car_availability_data');

async function create_daily_booking_slack_message(booking_data, car_data, forecast_data) {
  // GOAL
  const goal = 400;
  const goal_message = `🎯 Goal: ${goal}`;

  const { looker_forecast_dashboard_link } = await looker_links();

  const { formatted_car_data } = await get_formatted_car_avail_data(car_data);
  const [{
    count_total_available,
    count_total_on_rent,
    count_ncm_rp_out,
    count_ncm_dc_out,
    count_other,
    count_total_cars,
    utilization_total,
  }] = formatted_car_data;

  const { minBookingEstimate, maxBookingEstimate, actual_7_days_ago, average_last_7_days, average_same_day_last_4_weeks } = forecast_data.data;

  const { country_data, summary_data } = await get_country_data(booking_data);
  const { 
      yesterday_cancelled, 
      yesterday_not_cancelled, 
      yesterday_total, 
      today_cancelled, 
      today_not_cancelled, 
      today_total,  
  } = summary_data;

    // Use find to get the booking for the United Arab Emirates
    const uaeBookings = country_data.find(booking => booking.delivery_country === 'United Arab Emirates');

    // Extract only the booking values, with defaults in case not found
    const { today_not_cancelled: uae_bookings_today = 0, yesterday_not_cancelled: uae_bookings_yesterday = 0 } = uaeBookings || {};

    let { created_at_message, most_recent_booking_date_message, created_at_date_unformatted } = await date_info(booking_data);

    let { booking_count_message, today_above_below_goal_message, status_today } = await get_booking_status_today(booking_data, goal, uae_bookings_today);

    let { status_yesterday } = await get_booking_status_yesterday(booking_data, goal, uae_bookings_yesterday);
    
    const { pacing_message, pacing_status_message, pacing_threshold } = await check_pacing_for_current_hour(created_at_date_unformatted, uae_bookings_today);

    // FINAL MESSAGE
    const slackMessage = 
      `\n**************\n` +
      `BOOKING DATA\n` +
      `${created_at_message}\n` +
      `${most_recent_booking_date_message}\n` +
      `--------------\n`+
      `UAE BOOKING ESTIMATE\n` +
      `💡 MIN ESTIMATE: ${minBookingEstimate}, MAX ESTIMATE: ${maxBookingEstimate}\n` +
      `📋 ACTUAL: 7 DAYS AGO = ${actual_7_days_ago}, AVG LAST 7 DAYS = ${average_last_7_days}, AVG SAME DAY LAST 4 WEEKS = ${average_same_day_last_4_weeks}\n` +
      `--------------\n` +
      `UAE CARS\n` +
      `🚗 Avail: ${count_total_available}, On-Rent: ${count_total_on_rent}, Total: ${count_total_cars}, Util: ${utilization_total}\n` +
      `--------------\n`+
      `UAE BOOKINGS\n` +
      `${booking_count_message}\n` +
      `${pacing_message}\n` +
      `${pacing_status_message}\n` +
      `${goal_message}\n` +
      `${today_above_below_goal_message}\n` +
      `${status_today}\n` +
      `--------------\n`+
      `${status_yesterday}\n` +
      `--------------\n` +
      `${pacing_threshold}\n` +
      `**************\n` +
      `NET BOOKINGS\n` +
      `${yesterday_not_cancelled}\n${today_not_cancelled}\n` +
      `  ------------\n` +
      `CANCELLED BOOKINGS\n` +
      `${yesterday_cancelled}\n${today_cancelled}\n` +
      `  ------------\n` +
      `GROSS BOOKINGS\n` +
      `${yesterday_total}\n${today_total}\n` +
      `**************\n` 
      // +
      // `${looker_forecast_dashboard_link}` + `\n` +
      // `**************\n`
    ;

    console.log(slackMessage);

    return slackMessage;
}

// CREATE DATE INFO
async function date_info(booking_data) {

  // console.log(booking_data);
  
  // DATE INFO
  const created_at_date = `${getFormattedDateAmPm(booking_data[0].created_at_gst)} GST`;
  const created_at_message = `Info Queried At: ${created_at_date}`;
  const created_at_date_unformatted = booking_data[0].created_at_gst;

  // const most_recent = MAX(date_most_recent_created_on_gst, date_most_recent_updated_on_gst);
  const most_recent = new Date(Math.max(
    new Date(booking_data[0].date_most_recent_created_on_gst).getTime(),
    new Date(booking_data[0].date_most_recent_updated_on_gst).getTime()
  ));

  const most_recent_booking_date = `${getFormattedDateAmPm(most_recent)} GST`;
  const most_recent_booking_date_message = `Most Recent Booking At: ${most_recent_booking_date}`;

  return { created_at_date, created_at_message, most_recent_booking_date, most_recent_booking_date_message, created_at_date_unformatted };
}

// CREATE LOOKER STUDIO LINKS
async function looker_links() {
  const link_dashboard = `https://lookerstudio.google.com/u/0/reporting/20953aff-a544-445b-91ba-2f2d378a70c3/page/p_zsb11me7nd?pli=1`;
  const looker_forecast_dashboard_link = `<${link_dashboard}|Link to Looker Forecast Dashboard>`;

  return { looker_forecast_dashboard_link };
}

// BOOKINGS TODAY
async function get_booking_status_today(booking_data, goal, uae_bookings_today) {
  // Filter for entries related to the United Arab Emirates
  // const uae_bookings_today = booking_data.filter(booking => booking.delivery_country === 'United Arab Emirates')[1].current_bookings;

  let booking_count_message = `📢 Bookings: ${uae_bookings_today}`;
  const today_above_below_goal = (uae_bookings_today - goal);
  let today_above_below_goal_message = `${today_above_below_goal >= 0 ? `🟢 Above Goal: ${today_above_below_goal}` : `🔴 Below Goal: ${today_above_below_goal}`}`;
    
  let status_today = today_above_below_goal >= goal ? '✅🚀 Well done!' : '💪 Keep going!';

  return { uae_bookings_today, booking_count_message, today_above_below_goal_message, status_today };
}

// BOOKINGS YESTERDAY
async function get_booking_status_yesterday(booking_data, goal, uae_bookings_yesterday) {
  // const uae_bookings_yesterday = booking_data.filter(booking => 
  //   booking.delivery_country === 'United Arab Emirates')[0].current_bookings;

  const yesterday_above_below_goal = (uae_bookings_yesterday - goal);

  let status_yesterday = `Yesterday: ${yesterday_above_below_goal >= 0 ? `${uae_bookings_yesterday}. +${yesterday_above_below_goal} above goal. Well done🔥🔥!! ` : `${uae_bookings_yesterday}. ${yesterday_above_below_goal} below goal. We’re learning from this 👀!`}`;

  return { uae_bookings_yesterday, status_yesterday }
}

// Function to check pacing for the current hour
async function check_pacing_for_current_hour(created_at_date, uae_bookings_today) {
  const currentHour = new Date(created_at_date).getHours(); // Get the hour in created_at_date GST
  let currentHourFormatted = currentHour < 10 ? `0${currentHour}:00` : `${currentHour}:00`;

  // currentHourFormatted = '23:01'; // test pacing logic
  const target = await find_target_for_current_hour(currentHourFormatted);

  const pacing_message = `📈 Pacing Target: ${target}`;

  const pacing_status_message = 
    `${uae_bookings_today >= target ? `🌱 Above Pacing: +${uae_bookings_today - target}` : `🟡 Below Pacing: ${uae_bookings_today - target}`}`;

    
  // const pacing_threshold = `Pacing Goals: 8a = 25, 12n = 100, 2p = 150, 5p = 235, 7p = 295, 10p = 330, 12a = 350`;
  const pacing_threshold = `Pacing Goals: 8a = 30, 12n = 115, 2p = 175, 5p = 270, 7p = 340, 10p = 375, 12a = 400`;

  return { pacing_message, pacing_status_message, pacing_threshold };
}

// Function to find the closest hour
async function find_target_for_current_hour(currentHourFormatted) {
  const pacing_thresholds = {
    '00:00': 10,
    '08:00': 30,
    '12:00': 115,
    '14:00': 175,
    '17:00': 270,
    '19:00': 340,
    '22:00': 375,
    '24:00': 400,
  };

  const inputDate = new Date(`1970-01-01T${currentHourFormatted}:00Z`);
  let closestHour = null;
  let smallestDifference = Infinity;
  
  for (const hour in pacing_thresholds) {
    const thresholdDate = new Date(`1970-01-01T${hour}:00Z`);
    const difference = Math.abs(thresholdDate - inputDate) / (1000 * 60 * 60); // Convert to hours
    
    // console.log(currentHourFormatted, currentHourFormatted, hour, difference, smallestDifference);

      if (difference <= smallestDifference) {
          smallestDifference = difference;
          closestHour = hour;
      }
  }

  const target = pacing_thresholds[closestHour];
  // console.log('current hour ', currentHourFormatted, 'closest hour = ', closestHour, 'target = ', target);

  return target;
}

// // testing function
// async function main() {
//   const { execute_get_daily_booking_data} = require('../daily_booking_data/step_1_sql_get_daily_booking_data');
//   const { execute_get_car_availability } = require('../daily_car_availability_data/step_1_sql_get_car_availability');
//   const { execute_get_slack_forecast_data } = require('../daily_booking_forecast/step_3_get_slack_forecast_data');

//   const booking_data = await execute_get_daily_booking_data();
//   const car_data = await execute_get_car_availability();
//   const forecast_data = await execute_get_slack_forecast_data();

//   console.log('slack daily booking message', forecast_data);

//   create_daily_booking_slack_message(booking_data, car_data, forecast_data);
// }

// main();

module.exports = {
  create_daily_booking_slack_message,
}