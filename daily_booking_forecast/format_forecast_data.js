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

    // Extract booking total
    const booking_total = data
        .filter(item => 
            [
                'average_last_7_days', 
                'average_same_day_last_4_weeks',
                'actual_7_days_ago', 
            ]
            .includes(item.segment_minor))
        .map(item => Number(item.booking_total));
        
    const [
        booking_total_average_last_7_days, 
        booking_total_average_same_day_last_4_weeks, 
        booking_total_actual_7_days_ago
    ] = booking_total;

    // Extract booking total
    const booking_total_prior_to_current_hour = data
        .filter(item => 
            [
                'average_last_7_days', //145
                'average_same_day_last_4_weeks', //134
                'actual_7_days_ago', // 158
                'actual_today', // 144
            ]
            .includes(item.segment_minor))
        .map(item => Number(item.booking_total_prior_to_current_hour));
        
    const [
        booking_current_hour_average_last_7_days, 
        booking_current_hour_average_same_day_last_4_weeks, 
        booking_current_hour_actual_7_days_ago,
        booking_current_hour_actual_today,
    ] = booking_total_prior_to_current_hour;
        
    return { 
        minBookingEstimate, 
        maxBookingEstimate,
        booking_total_average_last_7_days, 
        booking_total_average_same_day_last_4_weeks, 
        booking_total_actual_7_days_ago, 
        booking_current_hour_average_last_7_days, 
        booking_current_hour_average_same_day_last_4_weeks, 
        booking_current_hour_actual_7_days_ago,
        booking_current_hour_actual_today,
    };
}

// get_formatted_forcast_data(seed_forecast_data);

module.exports = {
    get_formatted_forcast_data,
};
