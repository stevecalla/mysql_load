const dayjs = require('dayjs');
const { getFormattedDate } = require('../utilities/getCurrentDate');

const bookings = [
    {
      booking_date_gst: null,
      delivery_country: null,
      current_bookings: 356,
      created_at_gst: '2024-11-14 05:08:01',
      most_recent_event_update: '2024-11-14 03:49:21',
      date_most_recent_created_on_gst: '2024-11-14 03:33:15',
      date_most_recent_updated_on_gst: '2024-11-14 04:46:36'
    },
    {
      booking_date_gst: '2024-11-13',
      delivery_country: null,
      current_bookings: 339,
      created_at_gst: '2024-11-14 05:08:01',
      most_recent_event_update: '2024-11-14 03:12:36',
      date_most_recent_created_on_gst: '2024-11-14 03:33:15',
      date_most_recent_updated_on_gst: '2024-11-14 04:46:36'
    },
    {
      booking_date_gst: '2024-11-13',
      delivery_country: 'Bahrain',
      current_bookings: 4,
      created_at_gst: '2024-11-14 05:08:01',
      most_recent_event_update: '2024-11-14 00:41:17',
      date_most_recent_created_on_gst: '2024-11-14 03:33:15',
      date_most_recent_updated_on_gst: '2024-11-14 04:46:36'
    },
    {
      booking_date_gst: '2024-11-13',
      delivery_country: 'Qatar',
      current_bookings: 2,
      created_at_gst: '2024-11-14 05:08:01',
      most_recent_event_update: '2024-11-13 17:30:07',
      date_most_recent_created_on_gst: '2024-11-14 03:33:15',
      date_most_recent_updated_on_gst: '2024-11-14 04:46:36'
    },
    {
      booking_date_gst: '2024-11-13',
      delivery_country: 'Saudia Arabia',
      current_bookings: 9,
      created_at_gst: '2024-11-14 05:08:01',
      most_recent_event_update: '2024-11-14 00:09:51',
      date_most_recent_created_on_gst: '2024-11-14 03:33:15',
      date_most_recent_updated_on_gst: '2024-11-14 04:46:36'
    },
    {
      booking_date_gst: '2024-11-13',
      delivery_country: 'United Arab Emirates',
      current_bookings: 324,
      created_at_gst: '2024-11-14 05:08:01',
      most_recent_event_update: '2024-11-14 03:12:36',
      date_most_recent_created_on_gst: '2024-11-14 03:33:15',
      date_most_recent_updated_on_gst: '2024-11-14 04:46:36'
    },
    {
      booking_date_gst: '2024-11-14',
      delivery_country: null,
      current_bookings: 17,
      created_at_gst: '2024-11-14 05:08:01',
      most_recent_event_update: '2024-11-14 03:49:21',
      date_most_recent_created_on_gst: '2024-11-14 03:33:15',
      date_most_recent_updated_on_gst: '2024-11-14 04:46:36'
    },
    {
      booking_date_gst: '2024-11-14',
      delivery_country: 'Saudia Arabia',
      current_bookings: 3,
      created_at_gst: '2024-11-14 05:08:01',
      most_recent_event_update: '2024-11-14 03:49:21',
      date_most_recent_created_on_gst: '2024-11-14 03:33:15',
      date_most_recent_updated_on_gst: '2024-11-14 04:46:36'
    },
    {
      booking_date_gst: '2024-11-14',
      delivery_country: 'United Arab Emirates',
      current_bookings: 14,
      created_at_gst: '2024-11-14 05:08:01',
      most_recent_event_update: '2024-11-14 03:43:31',
      date_most_recent_created_on_gst: '2024-11-14 03:33:15',
      date_most_recent_updated_on_gst: '2024-11-14 04:46:36'
    }
];

async function group_by_country(bookings) {
    // Get today's and yesterday's date
    const today = getFormattedDate(bookings[0].created_at_gst); // '2024-10-19'
    const yesterday = dayjs(today).subtract(1, 'day').format('YYYY-MM-DD'); // '2024-10-18'
    
    // console.log('today =', today, 'yesterday =', yesterday);

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
    
    // console.log('yesterdaySummary =', yesterdaySummary);
    // console.log('todaySummary =', todaySummary);
    // console.log('uae_bookings_today =', uae_bookings_today);
    // console.log('uae_bookings_yesterday =', uae_bookings_yesterday);

    return { yesterdaySummary, todaySummary, uae_bookings_today, uae_bookings_yesterday };
}

// group_by_country(bookings);

module.exports = {
    group_by_country,
}