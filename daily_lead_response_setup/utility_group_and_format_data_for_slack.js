const dayjs = require('dayjs');
const { lead_data } = require('./seed_data_120624');

async function create_summary(data, option, segmentField, is_value_only) {
    let summary = '';

    data.forEach(item => {
        const segmentValue = item[segmentField] || "Unknown";
        const value = item[option] || 0; // Get the value dynamically

        // Append to the summary
        is_value_only ? (summary += `${value}, `) : (summary += `${segmentValue}: ${value}, `);
    });

    return summary.slice(0, -2); // Remove trailing comma and space
}

async function sort_segment(data, criteria) {
    // Validate criteria to be either 'renting_in_country' or 'source_name'
    if (criteria !== 'renting_in_country' && criteria !== 'source_name' && criteria !== 'shift' && criteria !== 'response_time_bin') {
        throw new Error("Invalid sort criteria. Use 'renting_in_country' or 'source_name'.");
    }

    // Sort based on the provided criteria
    return data.sort((a, b) => {
        if (a[criteria] < b[criteria]) return -1;
        if (a[criteria] > b[criteria]) return 1;
        return 0;
    });
}

async function format_table(data) {
    // source: https://knock.app/blog/how-to-render-tables-in-slack-markdown

    if (!data || data.length === 0) {
      return "No data provided";
    }
  
    // Extract headers from the first object's keys
    const headers = Object.keys(data[0]);
  
    // Calculate the maximum width for each column
    const columnWidths = headers.map((header) =>
      Math.max(
        ...data.map((row) => row[header].toString().length),
        header.length,
      ),
    );
  
    // Create the divider
    const divider =
      "+" +
      headers.map((header, i) => "-".repeat(columnWidths[i] + 2)).join("+") +
      "+";
  
    // Create the header row
    const headerRow =
      "|" +
      headers
        .map((header, i) => ` ${header.padEnd(columnWidths[i])} `)
        .join("|") +
      "|";
  
    // Generate each row of data
    const rows = data.map(
      (row) =>
        "|" +
        headers
          .map(
            (header, i) => ` ${row[header].toString().padEnd(columnWidths[i])} `,
          )
          .join("|") +
        "|",
    );
  
    // Assemble the full table
    return [divider, headerRow, divider, ...rows, divider].join("\n");
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

async function rollup_by_segment(data, segmentField) {
    // Create a result object to store the rolled-up data by `created_on_pst` and segmentField
    const result = {};

    // Step 1: Iterate over the data and aggregate by `created_on_pst` and segmentField
    data.forEach(item => {
        const {
            created_on_pst,
            count_leads_total,
            count_leads_invalid,
            count_leads_valid,
            count_booking_id_cancelled_total,
            count_booking_id_not_cancelled_total,
            count_booking_id_total,
            count_booking_same_day_rental_status_cancelled_distinct,
            count_booking_same_day_rental_status_not_cancelled_distinct,
            count_booking_same_day_rental_status_distinct_total,
        } = item;

        const segmentValue = item[segmentField] || "Unknown"; // Get the segment value dynamically

        // Create a unique key based on the date and segment value
        const key = `${created_on_pst}_${segmentValue}`;

        // If the key does not exist in the result, initialize it
        if (!result[key]) {
            result[key] = {
                created_on_pst,
                [segmentField]: segmentValue,
                leads_total: 0,
                leads_invalid: 0,
                leads_valid: 0,
                count_booking_id_cancelled_total: 0,
                count_booking_id_not_cancelled_total: 0,
                count_booking_id_total: 0,
                count_booking_same_day_rental_status_cancelled_distinct: 0,
                count_booking_same_day_rental_status_not_cancelled_distinct: 0,
                count_booking_same_day_rental_status_distinct_total: 0,
            };
        }

        // Aggregate values
        result[key].leads_total += count_leads_total;
        result[key].leads_invalid += count_leads_invalid;
        result[key].leads_valid += count_leads_valid;
        result[key].count_booking_id_cancelled_total += count_booking_id_cancelled_total;
        result[key].count_booking_id_not_cancelled_total += count_booking_id_not_cancelled_total;
        result[key].count_booking_id_total += count_booking_id_total;
        result[key].count_booking_same_day_rental_status_cancelled_distinct += count_booking_same_day_rental_status_cancelled_distinct;
        result[key].count_booking_same_day_rental_status_not_cancelled_distinct += count_booking_same_day_rental_status_not_cancelled_distinct;
        result[key].count_booking_same_day_rental_status_distinct_total += count_booking_same_day_rental_status_distinct_total;
    });

    // Step 2: Convert the result object to an array
    return Object.values(result);
}

async function create_table_output(data, segmentField) {
    // Initialize totals for columns
    const columnTotals = {
        [segmentField]: "Total", // Label for the totals row
        All: 0,
        Valid: 0,
        Conf: 0,
        "% Conv": "",
        Same: 0,
        "% Conv ": "",
    };

    // Map data and calculate row totals
    const tableData = data.map(item => {
        const formatValue = (value) => (value === 0 || value === "0%") ? "" : value;

        const row = {
            [segmentField]: item[segmentField] || "Unknown",
            All: formatValue(item.leads_total || 0),
            Valid: formatValue(item.leads_valid || 0),
            Conf: formatValue(item.count_booking_id_not_cancelled_total || 0),
            "% Conv": formatValue(conversion(item.count_booking_id_not_cancelled_total, item.leads_valid)),
            Same: formatValue(item.count_booking_same_day_rental_status_not_cancelled_distinct || 0),
            "% Conv ": formatValue(conversion(item.count_booking_same_day_rental_status_not_cancelled_distinct, item.leads_valid)),
        };

        // Update column totals
        columnTotals.All += item.leads_total || 0;
        columnTotals.Valid += item.leads_valid || 0;
        columnTotals.Conf += item.count_booking_id_not_cancelled_total || 0;
        columnTotals.Same += item.count_booking_same_day_rental_status_not_cancelled_distinct || 0;

        return row;
    });

    // Add a column total for "% Conv" and "% Conv "
    columnTotals["% Conv"] = conversion(columnTotals.Conf, columnTotals.Valid);
    columnTotals["% Conv "] = conversion(columnTotals.Same, columnTotals.Valid);

    // Format the totals row
    for (const key in columnTotals) {
        if (key !== segmentField && key !== "% Conv" && key !== "% Conv ") {
            columnTotals[key] = columnTotals[key] === 0 ? "" : columnTotals[key];
        }
    }

    // Add a "Total" column to each row
    tableData.forEach(row => {
        row.Total = row.All || ""; // Use "All" as the row total for simplicity, replace 0 with ""
    });

    // Add the overall total for the "Total" column
    columnTotals.Total = columnTotals.All || "";

    // Append the totals row at the end
    tableData.push(columnTotals);

    // Format and return the table
    return format_table(tableData);
}

async function create_response_time_vs_shift_lead_table(data) {
    // Initialize the result object
    const pivotTable = {};

    // Step 1: Aggregate data by response_time_bin and shift
    data.forEach(item => {
        const responseTimeBin = item.response_time_bin || "Unknown";
        const shift = item.shift || "Unknown";

        // Initialize the row if it doesn't exist
        if (!pivotTable[responseTimeBin]) {
            pivotTable[responseTimeBin] = { response_time_bin: responseTimeBin, total: 0 };
        }

        // Add the count for this shift
        if (!pivotTable[responseTimeBin][shift]) {
            pivotTable[responseTimeBin][shift] = 0;
        }

        pivotTable[responseTimeBin][shift] += item.count_leads_valid || 0; // Use confirmed/valid leads
        pivotTable[responseTimeBin].total += item.count_leads_valid || 0; // Update row total
    });

    // Step 2: Convert the pivotTable object into an array and calculate column totals
    const allShifts = ["AM: 12a-8a", "Day: 8a-4p ", "Night: 4p-12a"];
    const columnTotals = { response_time_bin: "Total", total: 0 };

    const formatValue = (value) => (value === 0 || value === "0%") ? "" : value;

    const tableData = Object.values(pivotTable).map(row => {
        allShifts.forEach(shift => {
            if (!row[shift]) {
                row[shift] = 0; // Fill missing columns with 0
            }
            columnTotals[shift] = (columnTotals[shift] || 0) + row[shift]; // Update column total
        });
        columnTotals.total += row.total; // Update overall total

        // Format row values
        allShifts.forEach(shift => {
            row[shift] = formatValue(row[shift]);
        });
        row.total = formatValue(row.total);

        return row;
    });

    // Add the column totals as the last row
    allShifts.forEach(shift => {
        columnTotals[shift] = formatValue(columnTotals[shift]);
    });
    columnTotals.total = formatValue(columnTotals.total);
    tableData.push(columnTotals);

    // Step 3: Sort rows by response_time_bin
    tableData.sort((a, b) => {
        if (a.response_time_bin === "Total") return 1; // Ensure "Grand Total" is always last
        if (b.response_time_bin === "Total") return -1;
        return a.response_time_bin.localeCompare(b.response_time_bin); // Alphabetical order
    });

    // Step 4: Sort column headers (shifts)
    const sortedTableData = tableData.map(row => {
        const sortedRow = {};
        sortedRow.response_time_bin = row.response_time_bin;
        allShifts.forEach(shift => {
            sortedRow[shift] = row[shift]; // Preserve sorted column order
        });
        sortedRow.total = row.total; // Append row total
        return sortedRow;
    });

    // Step 5: Format the table
    return format_table(sortedTableData);
}

async function create_response_time_vs_shift_booking_table(data) {
    // Initialize the result object
    const pivotTable = {};

    // Step 1: Aggregate data by response_time_bin and shift
    data.forEach(item => {
        const responseTimeBin = item.response_time_bin || "Unknown";
        const shift = item.shift || "Unknown";

        // Initialize the row if it doesn't exist
        if (!pivotTable[responseTimeBin]) {
            pivotTable[responseTimeBin] = { response_time_bin: responseTimeBin, total: 0 };
        }

        // Add the count for this shift
        if (!pivotTable[responseTimeBin][shift]) {
            pivotTable[responseTimeBin][shift] = 0;
        }

        pivotTable[responseTimeBin][shift] += item.count_booking_id_not_cancelled_total || 0; // Use bookings
        pivotTable[responseTimeBin].total += item.count_booking_id_not_cancelled_total || 0; // Update row total
    });

    // Step 2: Convert the pivotTable object into an array and calculate column totals
    const allShifts = ["AM: 12a-8a", "Day: 8a-4p ", "Night: 4p-12a"];
    const columnTotals = { response_time_bin: "Total", total: 0 };

    const formatValue = (value) => (value === 0 || value === "0%") ? "" : value;

    const tableData = Object.values(pivotTable).map(row => {
        allShifts.forEach(shift => {
            if (!row[shift]) {
                row[shift] = 0; // Fill missing columns with 0
            }
            columnTotals[shift] = (columnTotals[shift] || 0) + row[shift]; // Update column total
        });
        columnTotals.total += row.total; // Update overall total

        // Format row values
        allShifts.forEach(shift => {
            row[shift] = formatValue(row[shift]);
        });
        row.total = formatValue(row.total);

        return row;
    });

    // Add the column totals as the last row
    allShifts.forEach(shift => {
        columnTotals[shift] = formatValue(columnTotals[shift]);
    });
    columnTotals.total = formatValue(columnTotals.total);
    tableData.push(columnTotals);

    // Step 3: Sort rows by response_time_bin
    tableData.sort((a, b) => {
        if (a.response_time_bin === "Total") return 1; // Ensure "Grand Total" is always last
        if (b.response_time_bin === "Total") return -1;
        return a.response_time_bin.localeCompare(b.response_time_bin); // Alphabetical order
    });

    // Step 4: Sort column headers (shifts)
    const sortedTableData = tableData.map(row => {
        const sortedRow = {};
        sortedRow.response_time_bin = row.response_time_bin;
        allShifts.forEach(shift => {
            sortedRow[shift] = row[shift]; // Preserve sorted column order
        });
        sortedRow.total = row.total; // Append row total
        return sortedRow;
    });

    // Step 5: Format the table
    return format_table(sortedTableData);
}

// async function create_response_time_vs_shift_conversion_table(data) {
//     // Initialize the result object
//     const pivotTable = {};

//     // Step 1: Aggregate data by response_time_bin and shift
//     data.forEach(item => {
//         const responseTimeBin = item.response_time_bin || "Unknown";
//         const shift = item.shift || "Unknown";

//         // Initialize the row if it doesn't exist
//         if (!pivotTable[responseTimeBin]) {
//             pivotTable[responseTimeBin] = {
//                 response_time_bin: responseTimeBin,
//                 total_valid_leads: 0,
//                 total_not_cancelled: 0,
//                 conversion: "0%",
//             };
//         }

//         // Initialize the shift column if it doesn't exist
//         if (!pivotTable[responseTimeBin][shift]) {
//             pivotTable[responseTimeBin][shift] = {
//                 valid_leads: 0,
//                 not_cancelled: 0,
//             };
//         }

//         // Update counts for the current shift
//         pivotTable[responseTimeBin][shift].valid_leads += item.count_leads_valid || 0;
//         pivotTable[responseTimeBin][shift].not_cancelled += item.count_booking_id_not_cancelled_total || 0;

//         // Update row totals
//         pivotTable[responseTimeBin].total_valid_leads += item.count_leads_valid || 0;
//         pivotTable[responseTimeBin].total_not_cancelled += item.count_booking_id_not_cancelled_total || 0;
//     });

//     // Step 2: Calculate column totals and prepare the table
//     const allShifts = ["AM: 12a-8a", "Day: 8a-4p ", "Night: 4p-12a"];
//     const columnTotals = {
//         response_time_bin: "Total",
//         total_valid_leads: 0,
//         total_not_cancelled: 0,
//         conversion: "0%",
//     };

//     const tableData = Object.values(pivotTable).map(row => {
//         const rowData = {
//             response_time_bin: row.response_time_bin,
//         };

//         allShifts.forEach(shift => {
//             const shiftData = row[shift] || { valid_leads: 0, not_cancelled: 0 };

//             // Calculate conversion percentage for each shift
//             rowData[shift] = shiftData.valid_leads > 0
//                 ? ((shiftData.not_cancelled / shiftData.valid_leads) * 100).toFixed(0) + "%"
//                 : "";

//             // Update column totals
//             columnTotals[shift] = columnTotals[shift] || { valid_leads: 0, not_cancelled: 0 };
//             columnTotals[shift].valid_leads += shiftData.valid_leads;
//             columnTotals[shift].not_cancelled += shiftData.not_cancelled;

//             // Add to overall totals
//             columnTotals.total_valid_leads += shiftData.valid_leads;
//             columnTotals.total_not_cancelled += shiftData.not_cancelled;
//         });

//         // Calculate row conversion
//         rowData.total = row.total_valid_leads > 0
//             ? ((row.total_not_cancelled / row.total_valid_leads) * 100).toFixed(0) + "%"
//             : "";

//         return rowData;
//     });

//     // Calculate column conversion percentages
//     const formattedColumnTotals = {
//         response_time_bin: "Total",
//     };

//     allShifts.forEach(shift => {
//         const shiftTotals = columnTotals[shift];
//         formattedColumnTotals[shift] = shiftTotals.valid_leads > 0
//             ? ((shiftTotals.not_cancelled / shiftTotals.valid_leads) * 100).toFixed(0) + "%"
//             : "";
//     });

//     // Calculate overall conversion percentage
//     formattedColumnTotals.total = columnTotals.total_valid_leads > 0
//         ? ((columnTotals.total_not_cancelled / columnTotals.total_valid_leads) * 100).toFixed(0) + "%"
//         : "";

//     // Add column totals as the last row
//     tableData.push(formattedColumnTotals);

//     // Step 3: Sort rows by response_time_bin, keeping the total row last
//     const sortedTableData = tableData.filter(row => row.response_time_bin !== "Total").sort((a, b) => {
//         return a.response_time_bin.localeCompare(b.response_time_bin);
//     });

//     // Add the total row at the end
//     sortedTableData.push(formattedColumnTotals);

//     // Step 4: Format the table
//     return format_table(sortedTableData);
// }

async function create_response_time_vs_shift_conversion_table(data) {
    // Initialize the result object
    const pivotTable = {};

    // Step 1: Aggregate data by response_time_bin and shift
    data.forEach(item => {
        const responseTimeBin = item.response_time_bin || "Unknown";
        const shift = item.shift || "Unknown";

        // Initialize the row if it doesn't exist
        if (!pivotTable[responseTimeBin]) {
            pivotTable[responseTimeBin] = {
                response_time_bin: responseTimeBin,
                total_valid_leads: 0,
                total_not_cancelled: 0,
            };
        }

        // Initialize the shift column if it doesn't exist
        if (!pivotTable[responseTimeBin][shift]) {
            pivotTable[responseTimeBin][shift] = {
                valid_leads: 0,
                not_cancelled: 0,
            };
        }

        // Update counts for the current shift
        pivotTable[responseTimeBin][shift].valid_leads += item.count_leads_valid || 0;
        pivotTable[responseTimeBin][shift].not_cancelled += item.count_booking_id_not_cancelled_total || 0;

        // Update row totals
        pivotTable[responseTimeBin].total_valid_leads += item.count_leads_valid || 0;
        pivotTable[responseTimeBin].total_not_cancelled += item.count_booking_id_not_cancelled_total || 0;
    });

    // Step 2: Calculate column totals and prepare the table
    const allShifts = ["AM: 12a-8a", "Day: 8a-4p ", "Night: 4p-12a"];
    const columnTotals = {
        response_time_bin: "Total",
        total_valid_leads: 0,
        total_not_cancelled: 0,
    };

    const tableData = Object.values(pivotTable).map(row => {
        const rowData = {
            response_time_bin: row.response_time_bin,
        };

        allShifts.forEach(shift => {
            const shiftData = row[shift] || { valid_leads: 0, not_cancelled: 0 };

            // Calculate conversion percentage for each shift
            const conversion = shiftData.valid_leads > 0
                ? ((shiftData.not_cancelled / shiftData.valid_leads) * 100).toFixed(0) + "%"
                : "";
            rowData[shift] = conversion === "0%" ? "" : conversion;

            // Update column totals
            columnTotals[shift] = columnTotals[shift] || { valid_leads: 0, not_cancelled: 0 };
            columnTotals[shift].valid_leads += shiftData.valid_leads;
            columnTotals[shift].not_cancelled += shiftData.not_cancelled;

            // Add to overall totals
            columnTotals.total_valid_leads += shiftData.valid_leads;
            columnTotals.total_not_cancelled += shiftData.not_cancelled;
        });

        // Calculate row conversion
        const rowConversion = row.total_valid_leads > 0
            ? ((row.total_not_cancelled / row.total_valid_leads) * 100).toFixed(0) + "%"
            : "";
        rowData.total = rowConversion === "0%" ? "" : rowConversion;

        return rowData;
    });

    // Calculate column conversion percentages
    const formattedColumnTotals = {
        response_time_bin: "Total",
    };

    allShifts.forEach(shift => {
        const shiftTotals = columnTotals[shift];
        const shiftConversion = shiftTotals.valid_leads > 0
            ? ((shiftTotals.not_cancelled / shiftTotals.valid_leads) * 100).toFixed(0) + "%"
            : "";
        formattedColumnTotals[shift] = shiftConversion === "0%" ? "" : shiftConversion;
    });

    // Calculate overall conversion percentage
    const overallConversion = columnTotals.total_valid_leads > 0
        ? ((columnTotals.total_not_cancelled / columnTotals.total_valid_leads) * 100).toFixed(0) + "%"
        : "";
    formattedColumnTotals.total = overallConversion === "0%" ? "" : overallConversion;

    // Add column totals as the last row
    tableData.push(formattedColumnTotals);

    // Step 3: Sort rows by response_time_bin, keeping the total row last
    const sortedTableData = tableData.filter(row => row.response_time_bin !== "Total").sort((a, b) => {
        return a.response_time_bin.localeCompare(b.response_time_bin);
    });

    // Add the total row at the end
    sortedTableData.push(formattedColumnTotals);

    // Step 4: Format the table
    return format_table(sortedTableData);
}


async function group_and_format_data_for_slack(data, date = null, countryFilter = null) {
    // Step 1: Find the max created_on_pst
    const maxCreatedOnPst = data.reduce((max, item) => {
        return item.created_on_pst > max ? item.created_on_pst : max;
    }, data[0]?.created_on_pst || null);

    // Use the provided date parameter or the max created_on_pst as default
    const effectiveDate = date || maxCreatedOnPst;

    // Step 2: Filter data by date
    let filteredData = data.filter(item => item.created_on_pst === effectiveDate);

    // Step 3: Apply country filter if provided
    if (countryFilter) {
        filteredData = filteredData.filter(item => item.renting_in_country_abb === countryFilter);
    }

    // COUNTRY ROLLUP
    const country = 'renting_in_country';
    const country_rollup = await rollup_by_segment(filteredData, country);
    const country_sorted_rollup = await sort_segment(country_rollup, country);
    const country_table_output = await create_table_output(country_sorted_rollup, country);


    // SOURCE ROLLUP
    const source = 'source_name';
    const source_rollup = await rollup_by_segment(filteredData, source);
    const source_sorted_rollup = await sort_segment(source_rollup, source);
    const source_table_output = await create_table_output(source_sorted_rollup, source);

    // SHIFT ROLLUP
    const shift = 'shift';
    const shift_rollup = await rollup_by_segment(filteredData, shift);
    const shift_sorted_rollup = await sort_segment(shift_rollup, shift);
    const shift_table_output = await create_table_output(shift_sorted_rollup, shift);

    // RESPONSE TIME ROLLUP
    const response_time = 'response_time_bin';
    const response_time_rollup = await rollup_by_segment(filteredData, response_time);
    const response_time_sorted_rollup = await sort_segment(response_time_rollup, response_time);
    const response_time_table_output = await create_table_output(response_time_sorted_rollup, response_time);

    // RESPONSE TIME VS SHIFT - LEADS
    const response_time_by_shift_leads_output = await create_response_time_vs_shift_lead_table(filteredData);

    // RESPONSE TIME VS SHIFT - BOOKINGS
    const response_time_by_shift_bookings_output = await create_response_time_vs_shift_booking_table(filteredData);

    // RESPONSE TIME VS SHIFT - CONVERSION
    const response_time_by_shift_conversion_output = await create_response_time_vs_shift_conversion_table(filteredData);
    
    console.log(country_table_output);
    console.log(source_table_output);
    console.log(shift_table_output);
    console.log(response_time_table_output);    
    console.log(response_time_by_shift_leads_output);
    console.log(response_time_by_shift_bookings_output);
    console.log(response_time_by_shift_conversion_output);

    return { effectiveDate, countryFilter, country_table_output, source_table_output, shift_table_output, response_time_table_output, response_time_by_shift_leads_output, response_time_by_shift_bookings_output, response_time_by_shift_conversion_output }
}

// 2nd parameter is date ie 2024-12-05; 3rd parameter is count by first 3 characters or uae
// group_and_format_data_for_slack(lead_data, '', 'uae');

module.exports = {
    group_and_format_data_for_slack,
};
