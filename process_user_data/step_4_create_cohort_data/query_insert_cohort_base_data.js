const query_insert_cohort_base_data = `
-- Step 2: Insert data from ezhire_booking_data.booking_data into key_metrics table

INSERT INTO user_data_cohort_base (user_ptr_id, date_join_formatted_gst, date_join_cohort, booking_id, status, booking_type, vendor, is_repeat, country, booking_date, max_booking_datetime, pickup_date, pickup_datetime, return_date, return_datetime, extension_days, booking_charge_aed, booking_charge_less_discount_aed, extension_charge_aed, booking_charge_less_discount_extension_aed)

SELECT user.user_ptr_id, user.date_join_formatted_gst, user.date_join_cohort, booking_id, status, booking_type, marketplace_or_dispatch AS vendor, repeated_user AS is_repeat, deliver_country AS country, booking_date, max_booking_datetime, pickup_date, pickup_datetime, return_date, return_datetime, extension_days, booking_charge_aed, booking_charge_less_discount_aed, extension_charge_aed, booking_charge_less_discount_extension_aed

FROM ezhire_user_data.user_data_base AS user
    LEFT JOIN ezhire_booking_data.booking_data AS booking ON booking.customer_id = user.user_ptr_id;
`;

module.exports = { 
    query_insert_cohort_base_data 
};