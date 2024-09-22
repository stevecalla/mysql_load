async function create_daily_booking_slack_message(results) {
  // DATE INFO
  const created_at_date = results[0].created_at_gst;
  const created_at_message = `Info Updated At: ${created_at_date} GST`;

  const most_recent_booking_date = results[0].date_most_recent_created_on_gst;
  const most_recent_booking_date_message = `Most Recent Booking At: ${most_recent_booking_date} GST`;

  // GOAL
  const goal = 325;
  const goal_message = `🎯 Goal: ${goal}`;

  // BOOKINGS TODAY
  const bookings_today = results[7].current_bookings;

  let booking_count_message = `📢 Bookings: ${bookings_today}`;
  const today_above_below_goal = (bookings_today - goal);
  let today_above_below_goal_message = 
    `${today_above_below_goal >= 0 ? `🟢 Above Goal: ${today_above_below_goal}` : `🔴 Below Goal: ${today_above_below_goal}`}`;
    
  let status_today = 
    today_above_below_goal >= goal ? '✅🚀 Well done!' : '💪 Keep going!';

  // BOOKINGS YESTERDAY
  const bookings_yesterday = results[6].current_bookings;

  const yesterday_above_below_goal = (bookings_yesterday - goal);
  let status_yesterday = 
    `Yesterday: ${yesterday_above_below_goal >= 0 ? `= ${bookings_yesterday}. +${yesterday_above_below_goal} above goal. Well done🔥🔥!! ` : `${bookings_yesterday}. ${yesterday_above_below_goal} below goal. We’re learning from this 👀!`}`;

  // PACING
  const get_pacing_messages = await check_pacing_for_current_hour(created_at_date, bookings_today);
  const { pacing_message, pacing_status_message } = get_pacing_messages;
  const pacing_threshold = 
    `Pacing Goals: 8a = 25, 12n = 90, 2p = 135, 5p = 215, 7p = 270, 10p = 300, 12a = 325`;

  // FINAL MESSAGE
  const slackMessage = 
    `\n**************\nUAE ONLY\n${created_at_message}\n${most_recent_booking_date_message}\n--------------\n${booking_count_message}\n${pacing_message}\n${pacing_status_message}\n${goal_message}\n${today_above_below_goal_message}\n${status_today}\n--------------\n${pacing_threshold}\n--------------\n${status_yesterday}\n**************\n`;
  console.log(slackMessage);

  return slackMessage;
}

// Function to check pacing for the current hour
async function check_pacing_for_current_hour(created_at_date, bookings_today) {
  const currentHour = new Date(created_at_date).getHours(); // Get the hour in created_at_date GST
  let currentHourFormatted = currentHour < 10 ? `0${currentHour}:00` : `${currentHour}:00`;

  // currentHourFormatted = '21:05'; // test pacing logic
  const target = await find_target_for_current_hour(currentHourFormatted);

  const pacing_message = `📈 Pacing Target: ${target}`;

  const pacing_status_message = 
    `${bookings_today >= target ? `🌱 Above Pacing: +${bookings_today - target}` : `🟡 Below Pacing: ${bookings_today - target}`}`;

  return { pacing_message, pacing_status_message };
}

// Function to find the closest hour
async function find_target_for_current_hour(currentHourFormatted) {
  const pacing_thresholds = {
    '08:00': 25,
    '12:00': 90,
    '14:00': 135,
    '17:00': 215,
    '19:00': 270,
    '22:00': 300,
    '00:00': 325,
  };

  const inputDate = new Date(`1970-01-01T${currentHourFormatted}:00Z`);
  let closestHour = null;
  let smallestDifference = Infinity;
  
  for (const hour in pacing_thresholds) {
    const thresholdDate = new Date(`1970-01-01T${hour}:00Z`);
    const difference = Math.abs(thresholdDate - inputDate) / (1000 * 60 * 60); // Convert to hours
    
    // console.log(currentHourFormatted, inputDate, thresholdDate, difference, smallestDifference);

      if (difference < smallestDifference) {
          smallestDifference = difference;
          closestHour = hour;
      }
  }

  const target = closestHour === '00:00' ? 10 : pacing_thresholds[closestHour];
  console.log('closest hour = ', closestHour, 'target = ', target);

  return target;
}
  
module.exports = {
  create_daily_booking_slack_message,
}