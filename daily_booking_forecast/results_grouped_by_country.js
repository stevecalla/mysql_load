const dayjs = require('dayjs');
const { getFormattedDate } = require('../utilities/getCurrentDate');

// const bookings = [
//     {
//       booking_date_gst: null,
//       date_most_recent_created_on_gst: '2024-10-19 16:19:33',
//       current_bookings: 549,
//       created_at_gst: '2024-10-19 16:20:47',
//       delivery_country: null
//     },
//     {
//       booking_date_gst: '2024-10-18',
//       date_most_recent_created_on_gst: '2024-10-19 16:19:33',
//       current_bookings: 318,
//       created_at_gst: '2024-10-19 16:20:47',
//       delivery_country: null
//     },
//     {
//       booking_date_gst: '2024-10-18',
//       date_most_recent_created_on_gst: '2024-10-19 16:19:33',
//       current_bookings: 7,
//       created_at_gst: '2024-10-19 16:20:47',
//       delivery_country: 'Bahrain'
//     },
//     {
//       booking_date_gst: '2024-10-18',
//       date_most_recent_created_on_gst: '2024-10-19 16:19:33',
//       current_bookings: 1,
//       created_at_gst: '2024-10-19 16:20:47',
//       delivery_country: 'Qatar'
//     },
//     {
//       booking_date_gst: '2024-10-18',
//       date_most_recent_created_on_gst: '2024-10-19 16:19:33',
//       current_bookings: 11,
//       created_at_gst: '2024-10-19 16:20:47',
//       delivery_country: 'Saudia Arabia'
//     },
//     {
//       booking_date_gst: '2024-10-18',
//       date_most_recent_created_on_gst: '2024-10-19 16:19:33',
//       current_bookings: 299,
//       created_at_gst: '2024-10-19 16:20:47',
//       delivery_country: 'United Arab Emirates'
//     },
//     {
//       booking_date_gst: '2024-10-19',
//       date_most_recent_created_on_gst: '2024-10-19 16:19:33',
//       current_bookings: 231,
//       created_at_gst: '2024-10-19 16:20:47',
//       delivery_country: null
//     },
//     {
//       booking_date_gst: '2024-10-19',
//       date_most_recent_created_on_gst: '2024-10-19 16:19:33',
//       current_bookings: 5,
//       created_at_gst: '2024-10-19 16:20:47',
//       delivery_country: 'Bahrain'
//     },
//     {
//       booking_date_gst: '2024-10-19',
//       date_most_recent_created_on_gst: '2024-10-19 16:19:33',
//       current_bookings: 1,
//       created_at_gst: '2024-10-19 16:20:47',
//       delivery_country: 'Qatar'
//     },
//     {
//       booking_date_gst: '2024-10-19',
//       date_most_recent_created_on_gst: '2024-10-19 16:19:33',
//       current_bookings: 11,
//       created_at_gst: '2024-10-19 16:20:47',
//       delivery_country: 'Saudia Arabia'
//     },
//     {
//       booking_date_gst: '2024-10-19',
//       date_most_recent_created_on_gst: '2024-10-19 16:19:33',
//       current_bookings: 214,
//       created_at_gst: '2024-10-19 16:20:47',
//       delivery_country: 'United Arab Emirates'
//     }
// ];

async function group_by_country(bookings) {
    // Get today's and yesterday's date
    const today = getFormattedDate(bookings[0].created_at_gst); // '2024-10-19'
    const yesterday = dayjs(today).subtract(1, 'day').format('YYYY-MM-DD'); // '2024-10-18'
    
    console.log(today, yesterday);

    // Group by country
    const groupedBookings = bookings.reduce((acc, booking) => {
    const country = booking.delivery_country || 'total'; // Use 'Unknown' for null delivery_country
    const date = booking.booking_date_gst;
    
    // Initialize country object if it doesn't exist
    if (!acc[country]) {
        acc[country] = {
        yesterday: 0,
        today: 0,
        delivery_country: country
        };
    }
    
    // Check if the booking date is yesterday
    if (date === yesterday) {
        acc[country].yesterday += booking.current_bookings || 0;
    }
    
    // Check if the booking date is today
    if (date === today) {
        acc[country].today += booking.current_bookings || 0;
    }
    
    return acc;
    }, {});
    
    // Convert grouped object to array
    const resultArray = Object.values(groupedBookings);
    
    // Set default values to 0 if there are no bookings for a country
    const countryData = resultArray.map(countryData => ({
    delivery_country: countryData.delivery_country,
    yesterday: countryData.yesterday || 0,
    today: countryData.today || 0
    }));
    
    // console.log(countryData);

    let yesterdaySummary = '';
    let todaySummary = '';
  
    countryData.forEach(country => {
      const { delivery_country, yesterday, today } = country;
      // Get the first three characters in uppercase or 'UAE' for United Arab Emirates
      const countryCode = delivery_country === 'United Arab Emirates' ? 'UAE' : delivery_country.slice(0, 3).toUpperCase();
      yesterdaySummary += `${countryCode}: ${yesterday}, `;
      todaySummary += `${countryCode}: ${today}, `;
    });

    yesterdaySummary =  `🍿 Yesterday: ${yesterdaySummary.slice(0, -2)}`;
    todaySummary =      `⏳ Today:       ${todaySummary.slice(0, -2)}`;
  
    // console.log(yesterdaySummary); // Remove trailing comma and space
    // console.log(todaySummary); // Remove trailing comma and space  

    // Use find to get the booking for the United Arab Emirates
    const uaeBookings = countryData.find(booking => booking.delivery_country === 'United Arab Emirates');

    // Extract only the booking values, with defaults in case not found
    const { today: uae_bookings_today = 0, yesterday: uae_bookings_yesterday = 0 } = uaeBookings || {};
    
    return { yesterdaySummary, todaySummary, uae_bookings_today, uae_bookings_yesterday };
}

// group_by_country(bookings);

module.exports = {
    group_by_country,
}