const fs = require('fs').promises;
const mysql = require('mysql2/promise');
const config = require('../utilities/config');
const { schema_booking_table } = require('./schema_booking_table');
const { generateLogFile } = require('../utilities/generateLogFile');

console.log(config);
console.log(process.env);

async function loadData() {
  const pool = mysql.createPool(config.localDbConfig);
  let rowsAdded = 0;

  // Directory containing your CSV files
  const directory = config.csvExportPath;

  // Drop the table if it exists
  const dropTableQuery = 'DROP TABLE IF EXISTS booking_data';
  await pool.query(dropTableQuery);

  console.log('Table dropped successfully.');

  // Create the table
  await pool.query(schema_booking_table);
  console.log('Table created successfully.');

  try {
    // List all files in the directory
    const files = await fs.readdir(directory);
    let numberOfFiles = 0;
    console.log(files);

    // Iterate through each file
    for (const file of files) {
      if (file.endsWith('.csv')) {
        numberOfFiles++;
        // Construct the full file path
        const filePath = `${directory}${file}`;
        console.log(filePath);

        // Read the contents of the CSV file
        // const data = await fs.readFile(filePath, 'utf8');

        const loadDataQuery = `
        LOAD DATA INFILE '${filePath}'
        INTO TABLE booking_data
        FIELDS TERMINATED BY ','
        ENCLOSED BY '"'
        LINES TERMINATED BY '\\n'
        IGNORE 1 LINES
        (
            booking_id, 
            agreement_number,
            @booking_datetime, -- Variable to capture booking_datetime as string
            booking_year,
            booking_month,
            booking_day_of_month,
            booking_day_of_week,
            booking_day_of_week_v2,
            booking_time_bucket,
            @pickup_datetime, -- Variable to capture pickup_datetime as string
            pickup_year,
            pickup_month,
            pickup_day_of_month,
            pickup_day_of_week,
            pickup_day_of_week_v2,
            pickup_time_bucket,
            @return_datetime, -- Variable to capture return_datetime as string
            return_year,
            return_month,
            return_day_of_month,
            return_day_of_week,
            return_day_of_week_v2,
            return_time_bucket,
            status,
            booking_type,
            marketplace_or_dispatch,
            marketplace_partner,
            marketplace_partner_summary,
            booking_channel,
            booking_source,
            repeated_user,
            total_lifetime_booking_revenue,
            no_of_bookings,
            no_of_cancel_bookings,
            no_of_completed_bookings,
            no_of_started_bookings,
            customer_id,
            date_of_birth,
            age,
            customer_driving_country,
            customer_doc_vertification_status,
            days,
            extra_day_calc,
            customer_rate,
            insurance_rate,
            insurance_type,
            millage_rate,
            millage_cap_km,
            rent_charge,
            extra_day_charge,
            delivery_charge,
            collection_charge,
            additional_driver_charge,
            insurance_charge,
            intercity_charge,
            millage_charge,
            other_rental_charge,
            discount_charge,
            total_vat,
            other_charge,
            booking_charge,
            booking_charge_less_discount,
            booking_charge_aed,
            booking_charge_less_discount_aed,
            base_rental_revenue,
            non_rental_charge,
            extension_charge,
            is_extended,
            Promo_Code,
            promo_code_discount_amount,
            @promocode_created_date,  -- Variable to capture promocode_created_date as string
            promo_code_description,
            requested_car,
            car_name,
            make,
            color,
            deliver_country,
            deliver_city,
            country_id,
            city_id,
            delivery_location,
            deliver_method,
            delivery_lat,
            delivery_lng,
            collection_location,
            collection_method,
            collection_lat,
            collection_lng,
            nps_score,
            nps_comment
        )
        SET booking_datetime = STR_TO_DATE(@booking_datetime, "%Y-%m-%d %H:%i:%s"),
            pickup_datetime = STR_TO_DATE(@pickup_datetime, "%Y-%m-%d %H:%i:%s"),
            return_datetime = STR_TO_DATE(@return_datetime, "%Y-%m-%d %H:%i:%s");
            -- promocode_created_date = STR_TO_DATE(@promocode_created_date, "%Y-%m-%d %H:%i:%s");
            `

        // Execute the query with the data
        // const [loadDataResults] = await pool.query(loadDataQuery, [data]);
        const [loadDataResults] = await pool.query(loadDataQuery);
        const loadInfo = loadDataResults.info;
        console.log(`Data loaded successfully from ${filePath}.`);
        console.log(`Rows added: ${loadInfo}`);

        generateLogFile('loading_booking_data', `Rows added: ${loadInfo}`, config.csvExportPath);
        rowsAdded += parseInt(loadDataResults.affectedRows);
        console.log('Rows added = ', rowsAdded.toLocaleString());
      }
    }
    generateLogFile('loading_booking_data', `Total files added = ${numberOfFiles}`, config.csvExportPath);
    console.log('Files processed = ', numberOfFiles)

  } catch (error) {
    console.error('Error:', error);
    generateLogFile('loading_booking_data', `Error loading booking data: ${error}`, config.csvExportPath);
  } finally {
    generateLogFile('loading_booking_data', `Total rows added = ${rowsAdded.toLocaleString()}`, config.csvExportPath);
    // End the pool
    await pool.end();
  }
}

// Call the function
loadData();
