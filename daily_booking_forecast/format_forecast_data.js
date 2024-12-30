const dayjs = require('dayjs');
const { getFormattedDate } = require('../utilities/getCurrentDate');

const seed_forecast_data =
    [
        {
            segment_major: 'average',
            segment_minor: 'average_last_7_days',
            today_current_hour_gst: 14,
            booking_total_prior_to_current_hour: '145',
            booking_total: '337'
        },
        {
            segment_major: 'average',
            segment_minor: 'average_same_day_last_4_weeks',
            today_current_hour_gst: 14,
            booking_total_prior_to_current_hour: '134',
            booking_total: '323'
        },
        {
            segment_major: 'estimate',
            segment_minor: 'estimate_last_7_days',
            today_current_hour_gst: 14,
            booking_total_prior_to_current_hour: '144',
            booking_total: '336'
        },
        {
            segment_major: 'estimate',
            segment_minor: 'estimate_same_day_7_days_ago',
            today_current_hour_gst: 14,
            booking_total_prior_to_current_hour: '142',
            booking_total: '351'
        },
        {
            segment_major: 'estimate',
            segment_minor: 'estimate_same_day_last_4_weeks',
            today_current_hour_gst: 14,
            booking_total_prior_to_current_hour: '144',
            booking_total: '333'
        },
        {
            segment_major: 'actual',
            segment_minor: 'actual_7_days_ago',
            today_current_hour_gst: 14,
            booking_total_prior_to_current_hour: '158',
            booking_total: '367'
        },
        {
            segment_major: 'actual',
            segment_minor: 'actual_today',
            today_current_hour_gst: 14,
            booking_total_prior_to_current_hour: '144',
            booking_total: '150'
        }
    ];

async function get_formatted_forcast_data(data) {
    // Filter the data to only include "estimate" segment_major
    const estimateData = data.filter(item => item.segment_major === 'estimate');

    // Map to extract booking_total as numbers
    const bookingTotals = estimateData.map(item => Number(item.booking_total));

    // Find min and max
    const minBookingEstimate = Math.min(...bookingTotals);
    const maxBookingEstimate = Math.max(...bookingTotals);

    // Filter for the specific segment_minor values
    const filteredData = data
        .filter(item => 
            ['actual_7_days_ago', 'average_last_7_days', 'average_same_day_last_4_weeks'].includes(item.segment_minor)
        )
        .map(item => Number(item.booking_total)); // Extract booking_total as numbers 
        
        const [average_last_7_days, average_same_day_last_4_weeks, actual_7_days_ago] = filteredData;

    return { minBookingEstimate, maxBookingEstimate, actual_7_days_ago, average_last_7_days, average_same_day_last_4_weeks };
}

// get_formatted_forcast_data(seed_forecast_data);

module.exports = {
    get_formatted_forcast_data,
};
