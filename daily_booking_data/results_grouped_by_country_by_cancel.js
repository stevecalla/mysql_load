const dayjs = require('dayjs');
const { getFormattedDate } = require('../utilities/getCurrentDate');

// const bookings = [
//     {
//       booking_date_gst: '2024-11-13',
//       delivery_country: 'Bahrain',
//       booking_status_category: 'cancelled',
//       current_bookings: 3,
//       created_at_gst: '2024-11-14 05:12:58',
//       most_recent_event_update: '2024-11-13 20:06:59',
//       date_most_recent_created_on_gst: '2024-11-14 03:33:15',
//       date_most_recent_updated_on_gst: '2024-11-14 04:46:36'
//     },
//     {
//       booking_date_gst: '2024-11-13',
//       delivery_country: 'Bahrain',
//       booking_status_category: 'not_cancelled',
//       current_bookings: 4,
//       created_at_gst: '2024-11-14 05:12:58',
//       most_recent_event_update: '2024-11-14 00:41:17',
//       date_most_recent_created_on_gst: '2024-11-14 03:33:15',
//       date_most_recent_updated_on_gst: '2024-11-14 04:46:36'
//     },
//     {
//       booking_date_gst: '2024-11-13',
//       delivery_country: 'Qatar',
//       booking_status_category: 'cancelled',
//       current_bookings: 2,
//       created_at_gst: '2024-11-14 05:12:58',
//       most_recent_event_update: '2024-11-13 21:34:52',
//       date_most_recent_created_on_gst: '2024-11-14 03:33:15',
//       date_most_recent_updated_on_gst: '2024-11-14 04:46:36'
//     },
//     {
//       booking_date_gst: '2024-11-13',
//       delivery_country: 'Qatar',
//       booking_status_category: 'not_cancelled',
//       current_bookings: 2,
//       created_at_gst: '2024-11-14 05:12:58',
//       most_recent_event_update: '2024-11-13 17:30:07',
//       date_most_recent_created_on_gst: '2024-11-14 03:33:15',
//       date_most_recent_updated_on_gst: '2024-11-14 04:46:36'
//     },
//     {
//       booking_date_gst: '2024-11-13',
//       delivery_country: 'Saudia Arabia',
//       booking_status_category: 'not_cancelled',
//       current_bookings: 9,
//       created_at_gst: '2024-11-14 05:12:58',
//       most_recent_event_update: '2024-11-14 00:09:51',
//       date_most_recent_created_on_gst: '2024-11-14 03:33:15',
//       date_most_recent_updated_on_gst: '2024-11-14 04:46:36'
//     },
//     {
//       booking_date_gst: '2024-11-13',
//       delivery_country: 'United Arab Emirates',
//       booking_status_category: 'cancelled',
//       current_bookings: 28,
//       created_at_gst: '2024-11-14 05:12:58',
//       most_recent_event_update: '2024-11-14 01:58:35',
//       date_most_recent_created_on_gst: '2024-11-14 03:33:15',
//       date_most_recent_updated_on_gst: '2024-11-14 04:46:36'
//     },
//     {
//       booking_date_gst: '2024-11-13',
//       delivery_country: 'United Arab Emirates',
//       booking_status_category: 'not_cancelled',
//       current_bookings: 324,
//       created_at_gst: '2024-11-14 05:12:58',
//       most_recent_event_update: '2024-11-14 03:12:36',
//       date_most_recent_created_on_gst: '2024-11-14 03:33:15',
//       date_most_recent_updated_on_gst: '2024-11-14 04:46:36'
//     },
//     {
//       booking_date_gst: '2024-11-14',
//       delivery_country: 'Saudia Arabia',
//       booking_status_category: 'not_cancelled',
//       current_bookings: 3,
//       created_at_gst: '2024-11-14 05:12:58',
//       most_recent_event_update: '2024-11-14 03:49:21',
//       date_most_recent_created_on_gst: '2024-11-14 03:33:15',
//       date_most_recent_updated_on_gst: '2024-11-14 04:46:36'
//     },
//     {
//       booking_date_gst: '2024-11-14',
//       delivery_country: 'United Arab Emirates',
//       booking_status_category: 'cancelled',
//       current_bookings: 1,
//       created_at_gst: '2024-11-14 05:12:58',
//       most_recent_event_update: '2024-11-14 00:42:49',
//       date_most_recent_created_on_gst: '2024-11-14 03:33:15',
//       date_most_recent_updated_on_gst: '2024-11-14 04:46:36'
//     },
//     {
//       booking_date_gst: '2024-11-14',
//       delivery_country: 'United Arab Emirates',
//       booking_status_category: 'not_cancelled',
//       current_bookings: 14,
//       created_at_gst: '2024-11-14 05:12:58',
//       most_recent_event_update: '2024-11-14 03:43:31',
//       date_most_recent_created_on_gst: '2024-11-14 03:33:15',
//       date_most_recent_updated_on_gst: '2024-11-14 04:46:36'
//     }
// ];

async function create_summary(countryData, data = 'yesterday_cancelled') {
    let summary = '';
  
    countryData.forEach(country => {
        const { delivery_country, yesterday, today } = country;
        // Get the first three characters in uppercase or 'UAE' for United Arab Emirates
        const countryCode = delivery_country === 'United Arab Emirates' ? 'UAE' : delivery_country.slice(0, 3).toUpperCase();

        // Dynamically access the value using the 'data' key
        const value = country[data] || 0;  // Default to 0 if the key is not found

        // Append to summary
        summary += `${countryCode}: ${value}, `;
    });

    const text = {
        yesterday_cancelled: 'Yesterday - Cancelled',
        yesterday_not_cancelled: 'Yesterday - Net', // not cancelled
        yesterday_total: 'Yesterday - Gross',
        today_cancelled: 'Today - Cancelled',
        today_not_cancelled: 'Today - Net', // not cancelled
        today_total: 'Today - Gross', //grand total
    };

    const emoji = data.includes('today') ? '⏳' : '🍿';
    const text_v2 = data.includes('today') ? 'Today:       ' : 'Yesterday: ';

    summary =  `${emoji} ${text_v2} ${summary.slice(0, -2)}`;
    // console.log('summary =', summary);
    
    return summary
}

async function group_by_country_by_cancel(bookings) {
    const today = getFormattedDate(bookings[0].created_at_gst); // '2024-11-14'
    const yesterday = dayjs(today).subtract(1, 'day').format('YYYY-MM-DD'); // '2024-11-13'

    // console.log('today =', today, 'yesterday =', yesterday);

    // Group by country and booking status (Cancelled, Not Cancelled)
    const groupedBookings = bookings.reduce((acc, booking) => {
        const country = booking.delivery_country || 'Unknown'; // Use 'Unknown' for null delivery_country
        const date = booking.booking_date_gst;
        const status = booking.booking_status_category;

        // Initialize country object if it doesn't exist
        if (!acc[country]) {
            acc[country] = {
                cancelled_today: 0,
                cancelled_yesterday: 0,
                not_cancelled_today: 0,
                not_cancelled_yesterday: 0,
                delivery_country: country
            };
        }

        // Increment counts based on the status of the booking
        if (status === 'cancelled') {
            if (date === today) {
                acc[country].cancelled_today += booking.current_bookings || 0;
            }
            if (date === yesterday) {
                acc[country].cancelled_yesterday += booking.current_bookings || 0;
            }
        } else {
            if (date === today) {
                acc[country].not_cancelled_today += booking.current_bookings || 0;
            }
            if (date === yesterday) {
                acc[country].not_cancelled_yesterday += booking.current_bookings || 0;
            }
        }

        return acc;
    }, {});

    // Convert grouped object to array and calculate totals
    const resultArray = Object.values(groupedBookings);

    // Initialize global totals
    let globalTotals = {
        cancelled_today: 0,
        cancelled_yesterday: 0,
        not_cancelled_today: 0,
        not_cancelled_yesterday: 0,
        today_total: 0,
        total_yesterday: 0
    };

    // Set default values to 0, calculate totals for today and yesterday, and accumulate global totals
    const countryData = resultArray.map(countryData => {
        const total_yesterday = (countryData.cancelled_yesterday || 0) + (countryData.not_cancelled_yesterday || 0);
        const today_total = (countryData.cancelled_today || 0) + (countryData.not_cancelled_today || 0);

        // Accumulate global totals
        globalTotals.cancelled_today += countryData.cancelled_today || 0;
        globalTotals.cancelled_yesterday += countryData.cancelled_yesterday || 0;
        globalTotals.not_cancelled_today += countryData.not_cancelled_today || 0;
        globalTotals.not_cancelled_yesterday += countryData.not_cancelled_yesterday || 0;
        globalTotals.today_total += today_total;
        globalTotals.total_yesterday += total_yesterday;

        return {
            delivery_country: countryData.delivery_country,
            yesterday_cancelled: countryData.cancelled_yesterday || 0,
            yesterday_not_cancelled: countryData.not_cancelled_yesterday || 0,
            yesterday_total: total_yesterday,
            today_cancelled: countryData.cancelled_today || 0,
            today_not_cancelled: countryData.not_cancelled_today || 0,
            today_total: today_total
        };
    });

    // Add global totals to the result
    countryData.push({
        delivery_country: 'All Countries',
        yesterday_cancelled: globalTotals.cancelled_yesterday,
        yesterday_not_cancelled: globalTotals.not_cancelled_yesterday,
        yesterday_total: globalTotals.total_yesterday,
        today_cancelled: globalTotals.cancelled_today,
        today_not_cancelled: globalTotals.not_cancelled_today,
        today_total: globalTotals.today_total
    });

    // console.log('countryData =', countryData);

    await create_summary(countryData);

    return countryData;
}

async function get_country_data(bookings) {
    const country_data = await group_by_country_by_cancel(bookings);

    const data = [
        'yesterday_cancelled',
        'yesterday_not_cancelled',
        'yesterday_total',
        'today_cancelled',
        'today_not_cancelled',
        'today_total'
    ];

    let summary_data = {};
    for (i = 0; i < data.length; i++) {
        const formatted_output = await create_summary(country_data, data[i]);
        summary_data[data[i]] = formatted_output;
    }

    // console.log(summary_data);

    return { country_data, summary_data };
}

// get_country_data(bookings);

module.exports = {
    get_country_data,
};
