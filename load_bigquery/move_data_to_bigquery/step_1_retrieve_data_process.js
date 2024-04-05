const fs = require('fs');
const mysql = require('mysql2');
const dotenv = require('dotenv');
dotenv.config({ path: "../../.env" }); // adding the path ensures each folder will read the .env file as necessary

const { localBookingDbConfig, localKeyMetricsDbConfig, localPacingDbConfig, csvExportPath, } = require('../../utilities/config');
const { createLocalDBConnection } = require('../../utilities/connectionLocalDB');

const { getCurrentDateTime, getCurrentDateTimeForFileNaming } = require('../../utilities/getCurrentDate');
const { generateLogFile } = require('../../utilities/generateLogFile');

// DATE_FORMAT(booking_date, '%Y-%m-%d') AS booking_date
//converts 2022-11-08T07:00:00.000Z to '2022-11-08'
// DATE_FORMAT(CONVERT_TZ(created_at, '+00:00', '+00:00'), '%Y-%m-%d %H:%i:%s UTC') as created_at
// converts timestamp 2024-04-03T20:38:20.000Z to the format 2018-07-05 12:54:00 UTC 

const bookingQuery = `
    SELECT *
    FROM ezhire_booking_data.booking_data 
    -- WHERE status NOT LIKE '%Cancel%'
    -- AND pickup_year IN (2023, 2024)
    WHERE pickup_year IN (2023, 2024)
    ORDER BY booking_date ASC, pickup_date ASC
    -- LIMIT 1;
`;

const keyMetricsQuery = `
    SELECT
        DATE_FORMAT(CONVERT_TZ(created_at, '+00:00', '+00:00'), '%Y-%m-%d %H:%i:%s UTC') as created_at,
        calendar_date, -- selected as a string
        year,quarter,month,week,day,days_on_rent_whole_day,days_on_rent_fraction,trans_on_rent_count,booking_count,pickup_count,return_count,day_in_initial_period,day_in_extension_period,booking_charge_aed_rev_allocation,booking_charge_less_discount_aed_rev_allocation,rev_aed_in_initial_period,rev_aed_in_extension_period,vendor_on_rent_dispatch,vendor_on_rent_marketplace,booking_type_on_rent_daily,booking_type_on_rent_monthly,booking_type_on_rent_subscription,booking_type_on_rent_weekly,is_repeat_on_rent_no,is_repeat_on_rent_yes,country_on_rent_bahrain,country_on_rent_georgia,country_on_rent_kuwait,country_on_rent_oman,country_on_rent_pakistan,country_on_rent_qatar,country_on_rent_saudia_arabia,country_on_rent_serbia,country_on_rent_united_arab_emirates
    FROM ezhire_key_metrics.key_metrics_data
    ORDER BY calendar_date ASC
    LIMIT 5;
`;

const pacingQuery = `
    SELECT
        pickup_month_year,
        DATE_FORMAT(booking_date, '%Y-%m-%d') AS booking_date,
        days_from_first_day_of_month,
        count,
        total_booking_charge_aed,
        total_booking_charge_less_discount_aed,
        total_booking_charge_less_discount_extension_aed,
        total_extension_charge_aed,
        running_count,running_total_booking_charge_aed,
        running_total_booking_charge_less_discount_aed,
        running_total_booking_charge_less_discount_extension_aed,
        running_total_extension_charge_aed,
        DATE_FORMAT(CONVERT_TZ(created_at, '+00:00', '+00:00'), '%Y-%m-%d %H:%i:%s UTC') as created_at
    FROM ezhire_pacing_metrics.pacing_final_data
    ORDER BY pickup_month_year ASC, booking_date ASC
    -- LIMIT 5;
`;

// STEP #0 - MOVE FILES TO ARCHIVE
function moveFilesToArchive() {
    try {
        // List all files in the directory
        const files = fs.readdirSync(`${csvExportPath}bigquery`);
        console.log(files);

        // Create the "archive" directory if it doesn't exist
        const archivePath = `${csvExportPath}bigquery-archive`;
        fs.mkdirSync((archivePath), { recursive: true });

        // Iterate through each file
        for (const file of files) {
            if (file.endsWith('.csv')) {
                // Construct the full file paths
                const sourceFilePath = `${csvExportPath}bigquery/${file}`;
                const destinationFilePath = `${archivePath}/${file}`;

                // console.log(sourceFilePath);
                // console.log(destinationFilePath);

                try {
                    // Move the file to the "archive" directory
                    fs.renameSync(sourceFilePath, destinationFilePath);
                    // console.log(`Archived ${file}`);
                    generateLogFile('archive_big_query', `Archived ${file}`, csvExportPath);
                } catch (archiveErr) {
                    console.error(`Error moving file ${file} to archive:`, archiveErr);
                    generateLogFile('archive_big_query', `Error archive file ${file}: ${archiveErr}`, csvExportPath);
                }
            }
        }

    } catch (readErr) {
        console.error('Error reading files:', readErr);
    }
}

// STEP #1 - RETRIEVE BOOKING, KEY METRICS, PACING DATA
async function execute_get_data(pool, file_name, query) {
    return new Promise((resolve, reject) => {

        const startTime = performance.now();

        pool.query(query, (queryError, results) => {
            const endTime = performance.now();
            const elapsedTime = ((endTime - startTime) / 1_000).toFixed(2); //convert ms to sec

            if (queryError) {
                console.error('Error executing select query:', queryError);
                reject(queryError);
            } else {

                console.log(`GET DATA ${file_name}`);
                console.table(results);
                // console.log(Object.keys(results[0]));
                console.log(`Query results: ${results.length}, Elapsed Time: ${elapsedTime} sec`);
                resolve(results);
            }
        });
    });
}

// STEP #1.1a EXPORT RESULTS TO CSV FILE
function export_results_to_csv(results, file_name) {
    if (results.length === 0) {
        console.log('No results to export.');
        generateLogFile('load_big_query', 'No results to export.', csvExportPath);
        return;
    }

    // DEFINE DIRECTORY PATH
    const directoryPath = `${csvExportPath}bigquery`;
    // console.log('Directory path = ', directoryPath);

    // CHECK IF DIRECTORY EXISTS, IF NOT, CREATE IT
    if (!fs.existsSync(directoryPath)) {
        fs.mkdirSync(directoryPath, { recursive: true });
        // console.log(`Directory created: ${directoryPath}`);
    }

    //move files in bigquery directory to bigquery-archive

    try {
        const header = Object.keys(results[0]);

        const csvContent = `${header.join(',')}\n${results.map(row =>
            header.map(key => (row[key] !== null ? row[key] : 'NULL')).join(',')
        ).join('\n')}`;

        const createdAtFormatted = getCurrentDateTimeForFileNaming();
        const filePath = `${csvExportPath}bigquery/results_${createdAtFormatted}_${file_name}.csv`;
        // console.log('File path = ', filePath);

        fs.writeFileSync(filePath, csvContent);

        console.log(`Results exported to ${filePath}`);
        generateLogFile('load_big_query', `Results exported to ${filePath}`, csvExportPath);

    } catch (error) {
        console.error(`Error exporting results to csv:`, error);
        generateLogFile('load_big_query', `Error exporting results to csv: ${error}`, csvExportPath);
    }
}

// MAIN FUNCTION TO EXECUTE THE PROCESS
async function execute_retrieve_data() {
    try {
        const startTime = performance.now();

        // SET DATA OBJECT
        const getData = [
            // {
            //     poolName: localBookingDbConfig,
            //     fileName: 'booking_data',
            //     query: bookingQuery,
            // },
            {
                poolName: localKeyMetricsDbConfig,
                fileName: 'key_metrics_data',
                query: keyMetricsQuery,
            },
            // {
            //     poolName: localPacingDbConfig,
            //     fileName: 'pacing_data',
            //     query: pacingQuery,
            // },
        ];

        // STEP 1.0 ARCHIVE FILES
        console.log(`\nSTEP 1.0: ARCHIVE FILES`);
        console.log(`${getCurrentDateTime()}\n`);
        moveFilesToArchive();

        // STEP 1.1 PULL SQL DATA FROM BOOKING, KEY METRICS & PACING METRICS TABLES
        console.log(`\nSTEP 1.1: PULL SQL DATA FROM BOOKING DATA TABLE`);
        console.log(`${getCurrentDateTime()}\n`);

        for (let i = 0; i < getData.length; i++) {
            const { poolName, fileName, query } = getData[i];

            const pool = await createLocalDBConnection(poolName);

            let results = await execute_get_data(pool, fileName, query);

            // STEP 1.1a SAVE DATA TO CSV FILE
            console.log(`STEP 1.1a SAVE ${fileName} TO CSV FILE`);

            export_results_to_csv(results, fileName);

            // CLOSE POOL
            await pool.end(err => {
                if (err) {
                    console.error('Error closing connection pool:', err.message);
                } else {
                    console.log('Connection pool closed successfully.\n');
                }
            });
        }

        console.log('All queries executed successfully.');

        const endTime = performance.now();
        const elapsedTime = ((endTime - startTime) / 1_000).toFixed(2); //convert ms to sec

        return elapsedTime;

    } catch (error) {
        console.error('Error:', error);
    } finally {
        // End the pool
    }
}

// Run the main function
// execute_retrieve_data();

module.exports = {
    execute_retrieve_data,
}