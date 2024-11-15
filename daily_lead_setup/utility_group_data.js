const dayjs = require('dayjs');
const { getFormattedDate } = require('../utilities/getCurrentDate');
const { lead_data } = require('./seed_data');

async function create_summary(countryData, data = 'yesterday_cancelled') {
    let summary = '';
  
    countryData.forEach(country => {
        const { renting_in_country, yesterday, today } = country;
        // Get the first three characters in uppercase or 'UAE' for United Arab Emirates
        const countryCode = renting_in_country === 'United Arab Emirates' ? 'UAE' : renting_in_country.slice(0, 3).toUpperCase();

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

// async function rollup_by_country(data) {
//     // Create a result object to store the rolled-up data
//     const result = {};

//     // Track all unique dates and countries
//     const uniqueDates = new Set();
//     const uniqueCountries = new Set();

//     // Step 1: Gather all unique dates and countries from the data
//     data.forEach(item => {
//         const { created_on_gst, renting_in_country, count_leads } = item;
        
//         // Adjust renting_in_country: take first 3 characters, convert to uppercase or set to "Unknown" if null or empty
//         let countryCode = "";
        
//         // Special handling for "UNI" to convert it to "UAE"
//         if (renting_in_country.slice(0, 3).toUpperCase() === "UNI") {
//             countryCode = "UAE";
//         } else {
//             countryCode = (renting_in_country && renting_in_country.length > 0) 
//                 ? renting_in_country.slice(0, 3).toUpperCase() 
//                 : 'UNKNOWN'; 
//         }
        
//         // Add to unique sets
//         uniqueDates.add(created_on_gst);
//         uniqueCountries.add(countryCode);

//         const key = `${created_on_gst}_${countryCode}`;  // Create a unique key for each combination of created_on_gst and renting_in_country
        
//         // If the key does not exist in the result, initialize it
//         if (!result[key]) {
//             result[key] = {
//                 created_on_gst,
//                 renting_in_country: countryCode,
//                 total_leads: 0
//             };
//         }
        
//         // Add the count_leads to the total for that key
//         result[key].total_leads += count_leads;
//     });

//     // Step 2: Ensure each country has an entry for each date with total_leads set to 0 if missing
//     uniqueDates.forEach(date => {
//         uniqueCountries.forEach(country => {
//             const key = `${date}_${country}`;
            
//             // If the entry doesn't exist, initialize it with total_leads set to 0
//             if (!result[key]) {
//                 result[key] = {
//                     created_on_gst: date,  // Use the date from the loop to populate
//                     renting_in_country: country,
//                     total_leads: 0
//                 };
//             }
//         });
//     });

//     // Step 3: Convert the result object into an array of objects
//     const resultArray = Object.values(result);

//     // Step 4: Sort the result array by created_on_gst and then renting_in_country
//     resultArray.sort((a, b) => {
//         // First, sort by created_on_gst (date)
//         if (a.created_on_gst !== b.created_on_gst) {
//             return new Date(a.created_on_gst) - new Date(b.created_on_gst);
//         }
//         // If created_on_gst is the same, sort by renting_in_country
//         return a.renting_in_country.localeCompare(b.renting_in_country);
//     });

//     // Step 5: Add a single "total" entry for each date
//     const finalResult = [];
//     let currentDate = null;
//     let currentTotal = 0;

//     resultArray.forEach(item => {
//         // If the date changes, push the total for the previous date
//         if (currentDate && currentDate !== item.created_on_gst) {
//             finalResult.push({
//                 created_on_gst: currentDate,
//                 renting_in_country: "ALL",
//                 total_leads: currentTotal
//             });
//             currentTotal = 0;  // Reset the total for the next date
//         }

//         // Add the current item to the final result
//         finalResult.push(item);

//         // Accumulate the total leads for the current date
//         currentTotal += item.total_leads;

//         // Update the current date
//         currentDate = item.created_on_gst;
//     });

//     // Step 6: Add the total for the last date group
//     if (currentDate) {
//         finalResult.push({
//             created_on_gst: currentDate,
//             renting_in_country: "ALL",
//             total_leads: currentTotal
//         });
//     }

//     // Step 7: Return the sorted result with a single total for each date group
//     return finalResult;
// }

async function rollup_by_country(data) {
    // Create a result object to store the rolled-up data
    const result = {};

    // Track all unique dates and countries
    const uniqueDates = new Set();
    const uniqueCountries = new Set();

    // Step 1: Gather all unique dates and countries from the data
    data.forEach(item => {
        const { created_on_gst, renting_in_country, count_leads } = item;
        
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
                total_leads: 0
            };
        }
        
        // Add the count_leads to the total for that key
        result[key].total_leads += count_leads;
    });

    // Step 2: Ensure each country has an entry for each date with total_leads set to 0 if missing
    uniqueDates.forEach(date => {
        uniqueCountries.forEach(country => {
            const key = `${date}_${country}`;
            
            // If the entry doesn't exist, initialize it with total_leads set to 0
            if (!result[key]) {
                result[key] = {
                    created_on_gst: date,  // Use the date from the loop to populate
                    renting_in_country: country,
                    total_leads: 0
                };
            }
        });
    });

    // Step 3: Convert the result object into an array of objects
    const resultArray = Object.values(result);

    // Step 4: Sort the result array by created_on_gst and then renting_in_country
    resultArray.sort((a, b) => {
        // First, sort by created_on_gst (date)
        if (a.created_on_gst !== b.created_on_gst) {
            return new Date(a.created_on_gst) - new Date(b.created_on_gst);
        }
        // If created_on_gst is the same, sort by renting_in_country
        return a.renting_in_country.localeCompare(b.renting_in_country);
    });

    // Step 5: Find the min and max date
    const minDate = resultArray[0].created_on_gst;
    const maxDate = resultArray[resultArray.length - 1].created_on_gst;

    // Step 6: Add a single "total" entry for each date and label min/max dates as "Yesterday" and "Today"
    const finalResult = [];
    let currentDate = null;
    let currentTotal = 0;
    let dateGroup = []; // To hold country entries for a specific date

    resultArray.forEach(item => {
        // If the date changes, push the total for the previous date and reset for the new date
        if (currentDate && currentDate !== item.created_on_gst) {
            // Label the minimum date as "Yesterday" and the maximum date as "Today"
            if (currentDate === minDate) {
                currentDate = "Yesterday";
            } else if (currentDate === maxDate) {
                currentDate = "Today";
            }

            // Add the country-specific entries for the current date
            finalResult.push(...dateGroup);

            // Add the "ALL" entry for the current date group
            finalResult.push({
                created_on_gst: currentDate,
                renting_in_country: "ALL",
                total_leads: currentTotal
            });

            // Reset for the next date group
            currentTotal = 0;
            dateGroup = [];
        }

        // Update the current date
        currentDate = item.created_on_gst;

        // Label the minimum date as "Yesterday" and the maximum date as "Today"
        if (currentDate === minDate) {
            item.created_on_gst = "Yesterday";
        } else if (currentDate === maxDate) {
            item.created_on_gst = "Today";
        }

        // Add the current item to the date group
        dateGroup.push(item);

        // Accumulate the total leads for the current date
        currentTotal += item.total_leads;
    });

    // Step 7: Add the total for the last date group
    if (currentDate) {
        // Label the minimum date as "Yesterday" and the maximum date as "Today"
        if (currentDate === minDate) {
            currentDate = "Yesterday";
        } else if (currentDate === maxDate) {
            currentDate = "Today";
        }

        // Add the country-specific entries for the last date
        finalResult.push(...dateGroup);

        // Add the "ALL" entry for the last date group
        finalResult.push({
            created_on_gst: currentDate,
            renting_in_country: "ALL",
            total_leads: currentTotal
        });
    }

    // Step 8: Return the sorted result with a single total for each date group
    return finalResult;
}

async function format_lead_data(data) {
    const leads_rollup_by_country = await rollup_by_country(data);

    console.table(leads_rollup_by_country)

    // const data = [
    //     'yesterday_cancelled',
    //     'yesterday_not_cancelled',
    //     'yesterday_total',
    //     'today_cancelled',
    //     'today_not_cancelled',
    //     'today_total'
    // ];

    // let summary_data = {};
    // for (i = 0; i < data.length; i++) {
    //     const formatted_output = await create_summary(country_data, data[i]);
    //     summary_data[data[i]] = formatted_output;
    // }

    // // console.log(summary_data);

    return { data, leads_rollup_by_country };
}

format_lead_data(lead_data);

module.exports = {
    format_lead_data,
};
