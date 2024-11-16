const dayjs = require('dayjs');
const { getFormattedDate } = require('../utilities/getCurrentDate');
const { lead_data } = require('./seed_data');

async function create_summary(data, option) {
    let summary = '';
    let emoji = option.includes('today') ? '⏳' : '🍿';
    const day_label = option.includes('today') ? 'Today:       ' : 'Yesterday: ';

    if (data[0]?.renting_in_country) {
        data.forEach(item => {
            const { renting_in_country } = item;
    
            // Get the first three characters in uppercase or 'UAE' for United Arab Emirates
            const countryCode = renting_in_country === 'United Arab Emirates' ? 'UAE' : renting_in_country.slice(0, 3).toUpperCase();
    
            // Dynamically access the value using the 'option' key
            const value = item[option] || 0;  // Default to 0 if the key is not found
    
            // Append to summary
            summary += `${countryCode}: ${value}, `;
        })
    } else if (
        data.forEach(item => {
            const { source } = item;
    
            // Dynamically access the value using the 'option' key
            const value = item[option] || 0;  // Default to 0 if the key is not found
    
            // Append to summary
            summary += `${source}: ${value}, `;
        })
    );

    // console.log('emoji = ', emoji);
    // console.log('day label =', day_label);

    summary =  `${emoji} ${day_label} ${summary.slice(0, -2)}`;
    console.log(option, '=', summary);
    
    return summary
}

function conversion(booking_confirmed, leads) {
    let conversion = 0;

    // Ensure there's no division by zero
    if (leads > 0) {
        conversion = (booking_confirmed / leads) * 100;
    }

    // Format yesterday's conversion based on the value
    if (conversion === 0) {
        conversion = "0%";
    } else if (conversion >= 1) {
        conversion = conversion.toFixed(0) + "%";
    } else {
        conversion = conversion.toFixed(2) + "%";
    }

    return conversion;
}

async function rollup_by_country(data) {
    // Create a result object to store the rolled-up data for Yesterday and Today
    const result = {};

    // Track unique countries and dates
    const uniqueDates = new Set();
    const uniqueCountries = new Set();

    // Step 1: Gather all unique dates and countries from the data
    data.forEach(item => {
        const { created_on_gst, renting_in_country, count_leads, count_booking_cancelled, count_booking_confirmed, count_booking_total } = item;
        
        // Adjust renting_in_country: take first 3 characters, convert to uppercase or set to "Unknown" if null or empty
        let countryCode = "";
        
        // Special handling for "UNI" to convert it to "UAE"
        if (renting_in_country.slice(0, 3).toUpperCase() === "UNI") {
            countryCode = "UAE";
        } else {
            countryCode = (renting_in_country && renting_in_country.length > 0) 
                ? renting_in_country.slice(0, 3).toUpperCase() 
                : 'UNKNOWN'; 
        }
        
        // Add to unique sets
        uniqueDates.add(created_on_gst);
        uniqueCountries.add(countryCode);

        const key = `${created_on_gst}_${countryCode}`;  // Create a unique key for each combination of created_on_gst and renting_in_country
        
        // If the key does not exist in the result, initialize it
        if (!result[key]) {
            result[key] = {
                created_on_gst,
                renting_in_country: countryCode,
                total_leads: 0,
                count_booking_cancelled: 0,
                count_booking_confirmed: 0,
                count_booking_total: 0,
            };
        }
        
        // Add the count_leads to the total for that key
        result[key].total_leads += count_leads;

        result[key].count_booking_cancelled += count_booking_cancelled;

        // console.log('before = ', result[key], result[key].count_booking_confirmed)
        result[key].count_booking_confirmed += count_booking_confirmed;
        // console.log('after = ', result[key], result[key].count_booking_confirmed);

        result[key].count_booking_total += count_booking_total;
    });

    // Step 2: Ensure each country has an entry for "Yesterday" and "Today" with total_leads set to 0 if missing
    const minDate = Math.min(...Array.from(uniqueDates).map(d => new Date(d).getTime()));
    const maxDate = Math.max(...Array.from(uniqueDates).map(d => new Date(d).getTime()));
    const minDateStr = new Date(minDate).toISOString().split('T')[0]; // format as 'YYYY-MM-DD'
    const maxDateStr = new Date(maxDate).toISOString().split('T')[0]; // format as 'YYYY-MM-DD'

    uniqueCountries.forEach(country => {
        // Create entries for "Yesterday" and "Today" for each country
        if (!result[`${minDateStr}_${country}`]) {
            result[`${minDateStr}_${country}`] = {
                created_on_gst: "Yesterday",
                renting_in_country: country,
                total_leads: 0,
                count_booking_cancelled: 0,
                count_booking_confirmed: 0,
                count_booking_total: 0,
            };
        }
        if (!result[`${maxDateStr}_${country}`]) {
            result[`${maxDateStr}_${country}`] = {
                created_on_gst: "Today",
                renting_in_country: country,
                total_leads: 0,
                count_booking_cancelled: 0,
                count_booking_confirmed: 0,
                count_booking_total: 0,
            };
        }
    });

    console.log('unique countries = ', uniqueCountries)

    // Step 3: Organize the final output as [{ renting_in_country, yesterday: value, today: value }]
    const finalResult = [];
    let overallYesterdayTotal = 0;
    let overallTodayTotal = 0;
    let overallYesterdayBookingCancelled = 0;
    let overallTodayBookingCancelled = 0;
    let overallYesterdayBookingConfirmed = 0;
    let overallTodayBookingConfirmed = 0;
    let overallYesterdayBookingTotal = 0;
    let overallTodayBookingTotal = 0;

    uniqueCountries.forEach(country => {
        const yesterdayLeads = result[`${minDateStr}_${country}`] ? result[`${minDateStr}_${country}`].total_leads : 0;
        const todayLeads = result[`${maxDateStr}_${country}`] ? result[`${maxDateStr}_${country}`].total_leads : 0;

        const yesterday_booking_cancelled = result[`${minDateStr}_${country}`] ? result[`${minDateStr}_${country}`].count_booking_cancelled : 0;
        const today_booking_cancelled = result[`${maxDateStr}_${country}`] ? result[`${maxDateStr}_${country}`].count_booking_cancelled : 0;

        const yesterday_booking_confirmed = result[`${minDateStr}_${country}`] ? result[`${minDateStr}_${country}`].count_booking_confirmed : 0;
        const today_booking_confirmed = result[`${maxDateStr}_${country}`] ? result[`${maxDateStr}_${country}`].count_booking_confirmed : 0;

        const yesterday_booking_total = result[`${minDateStr}_${country}`] ? result[`${minDateStr}_${country}`].count_booking_total : 0;
        const today_booking_total = result[`${maxDateStr}_${country}`] ? result[`${maxDateStr}_${country}`].count_booking_total : 0;       
        
        const yesterdayBookingConversion = conversion(yesterday_booking_confirmed, yesterdayLeads) || "error";
        const todayBookingConversion = conversion(today_booking_confirmed, todayLeads) || "error";

        finalResult.push({
            renting_in_country: country,
            yesterday: yesterdayLeads,
            today: todayLeads,
            yesterday_booking_cancelled,
            yesterday_booking_confirmed,
            yesterday_booking_total,
            today_booking_cancelled,
            today_booking_confirmed,
            today_booking_total,
            // Convert to percentages and append '%' symbol
            yesterday_booking_conversion: yesterdayBookingConversion,
            today_booking_conversion: todayBookingConversion,
        });

        // Accumulate the overall totals for Yesterday and Today
        overallYesterdayTotal += yesterdayLeads;
        overallTodayTotal += todayLeads;

        overallYesterdayBookingCancelled += yesterday_booking_cancelled;
        overallTodayBookingCancelled += today_booking_cancelled;

        overallYesterdayBookingConfirmed += yesterday_booking_confirmed;
        overallTodayBookingConfirmed += today_booking_confirmed;

        overallYesterdayBookingTotal += yesterday_booking_total;
        overallTodayBookingTotal += today_booking_total;
    });

    // Step 4: Add the "ALL" total entry      
        
    const yesterdayBookingConversion = conversion(overallYesterdayBookingConfirmed, overallYesterdayTotal) || "error";
    const todayBookingConversion = conversion(overallTodayBookingConfirmed, overallTodayTotal) || "error";

    finalResult.push({
        renting_in_country: "ALL",
        yesterday: overallYesterdayTotal,
        today: overallTodayTotal,
        yesterday_booking_cancelled: overallYesterdayBookingCancelled,
        yesterday_booking_confirmed: overallYesterdayBookingConfirmed,
        yesterday_booking_total: overallYesterdayBookingTotal,
        today_booking_cancelled: overallTodayBookingCancelled,
        today_booking_confirmed: overallTodayBookingConfirmed,
        today_booking_total: overallTodayBookingTotal,
        // Conversion rates as percentages, multiply by 100
        yesterday_booking_conversion: yesterdayBookingConversion,
        today_booking_conversion: todayBookingConversion,
    });

    console.log(finalResult);

    // Step 5: Return the final result
    return finalResult;
}

async function rollup_by_source(data) {
    // Create a result object to store the rolled-up data for Yesterday and Today
    const result = {};

    // Track unique countries and dates
    const uniqueDates = new Set();
    const uniqueSource = new Set();

    // Step 1: Gather all unique dates and countries from the data
    data.forEach(item => {
        const { created_on_gst, source_name, count_leads } = item;
        
        // Add to unique sets
        uniqueDates.add(created_on_gst);
        uniqueSource.add(source_name);

        const key = `${created_on_gst}_${source_name}`;  // Create a unique key for each combination of created_on_gst and renting_in_country
        
        // If the key does not exist in the result, initialize it
        if (!result[key]) {
            result[key] = {
                created_on_gst,
                source: source_name,
                total_leads: 0
            };
        }
        
        // Add the count_leads to the total for that key
        result[key].total_leads += count_leads;
    });

    // Step 2: Ensure each source has an entry for "Yesterday" and "Today" with total_leads set to 0 if missing
    const minDate = Math.min(...Array.from(uniqueDates).map(d => new Date(d).getTime()));
    const maxDate = Math.max(...Array.from(uniqueDates).map(d => new Date(d).getTime()));
    const minDateStr = new Date(minDate).toISOString().split('T')[0]; // format as 'YYYY-MM-DD'
    const maxDateStr = new Date(maxDate).toISOString().split('T')[0]; // format as 'YYYY-MM-DD'

    uniqueSource.forEach(source => {
        // Create entries for "Yesterday" and "Today" for each source
        if (!result[`${minDateStr}_${source}`]) {
            result[`${minDateStr}_${source}`] = {
                created_on_gst: "Yesterday",
                source: source,
                total_leads: 0
            };
        }
        if (!result[`${maxDateStr}_${source}`]) {
            result[`${maxDateStr}_${source}`] = {
                created_on_gst: "Today",
                source_name: source,
                total_leads: 0
            };
        }
    });

    // Step 3: Organize the final output as [{ renting_in_country, yesterday: value, today: value }]
    const finalResult = [];
    let overallYesterdayTotal = 0;
    let overallTodayTotal = 0;

    uniqueSource.forEach(source => {
        const yesterdayLeads = result[`${minDateStr}_${source}`] ? result[`${minDateStr}_${source}`].total_leads : 0;
        const todayLeads = result[`${maxDateStr}_${source}`] ? result[`${maxDateStr}_${source}`].total_leads : 0;

        finalResult.push({
            source: source,
            yesterday: yesterdayLeads,
            today: todayLeads
        });

        // Accumulate the overall totals for Yesterday and Today
        overallYesterdayTotal += yesterdayLeads;
        overallTodayTotal += todayLeads;
    });

    // Step 4: Add the "ALL" total entry
    finalResult.push({
        source: "ALL",
        yesterday: overallYesterdayTotal,
        today: overallTodayTotal
    });

    // Step 5: Return the final result
    return finalResult;
}

async function sortLeads(data, criteria) {
    // Validate criteria to be either 'renting_in_country' or 'source_name'
    if (criteria !== 'renting_in_country' && criteria !== 'source') {
        throw new Error("Invalid sort criteria. Use 'renting_in_country' or 'source_name'.");
    }

    // Sort based on the provided criteria
    return data.sort((a, b) => {
        if (a[criteria] < b[criteria]) return -1;
        if (a[criteria] > b[criteria]) return 1;
        return 0;
    });
}

async function format_lead_data(data) {
    const options = [
        'yesterday', 
        'today',
        'yesterday_booking_cancelled',
        'yesterday_booking_confirmed',
        'yesterday_booking_total',
        'today_booking_cancelled',
        'today_booking_confirmed',
        'today_booking_total',
        'yesterday_booking_conversion',
        'today_booking_conversion',
    ];

    // COUNTRY ROLLUPS
    const leads_rollup_by_country = await rollup_by_country(data);
    const leads_sorted_by_country = await sortLeads(leads_rollup_by_country, 'renting_in_country');

    // ALL COUNTRIES WITH YESTERDAY & TODAY
    const all_countries_output_text = {};
    for (i = 0; i < options.length; i++) {
        const formatted_output = await create_summary(leads_sorted_by_country, options[i]);
        all_countries_output_text[options[i]] = formatted_output;
    }
    
    // UAE ONLY WITH YESTERDAY & TODAY
    let uae_only = leads_sorted_by_country.filter(({ renting_in_country }) => renting_in_country === 'UAE');

    const uae_only_output_text = {};
    for (i = 0; i < options.length; i++) {
        const formatted_output = await create_summary(uae_only, options[i]);
        uae_only_output_text[options[i]] = formatted_output;
    }

    // // SOURCE ROLLUPS
    // // ALL SOURCE DATA WITH YESTERDAY AND TODAY
    // const leads_rollup_by_source = await rollup_by_source(data);
    // const leads_sorted_by_source = await sortLeads(leads_rollup_by_source, 'source');

    // const source_output_text = {};
    // for (i = 0; i < options.length; i++) {
    //     const formatted_output = await create_summary(leads_sorted_by_source, options[i]);
    //     source_output_text[options[i]] = formatted_output;
    // }

    // // SOURCE UAE ONLY
    // let uae_only_source = data.filter(({ renting_in_country }) => renting_in_country === 'United Arab Emirates');
    // const leads_rollup_uae_only_by_source = await rollup_by_source(uae_only_source);
    // const leads_sorted_by_source_uae = await sortLeads(leads_rollup_uae_only_by_source, 'source');

    // const uae_only_source_output_text = {};
    // for (i = 0; i < options.length; i++) {
    //     const formatted_output = await create_summary(leads_sorted_by_source_uae, options[i]);
    //     uae_only_source_output_text[options[i]] = formatted_output;
    // }

    return { leads_rollup_by_country, all_countries_output_text, uae_only_output_text, source_output_text: [], uae_only_source_output_text: [] };
}

format_lead_data(lead_data);

module.exports = {
    format_lead_data,
};
