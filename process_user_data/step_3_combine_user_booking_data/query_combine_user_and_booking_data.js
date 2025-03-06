const query_combine_user_and_booking_data = `
-- CREATE user_data_combined_booking_data TABLE
CREATE TABLE user_data_combined_booking_data AS
    SELECT
        user.*,
        booking.booking_id, booking.agreement_number, booking.booking_date, booking.booking_datetime, booking.max_booking_datetime, booking.today, booking.booking_year, booking.booking_quarter, booking.booking_month, booking.booking_day_of_month, booking.booking_week_of_year, booking.booking_day_of_week, booking.booking_day_of_week_v2, booking.booking_time_bucket, booking.booking_count, booking.booking_count_excluding_cancel, booking.pickup_date, booking.pickup_datetime, booking.pickup_year, booking.pickup_quarter, booking.pickup_month, booking.pickup_day_of_month, booking.pickup_week_of_year, booking.pickup_day_of_week, booking.pickup_day_of_week_v2, booking.pickup_time_bucket, booking.return_date, booking.return_datetime, booking.return_year, booking.return_quarter, booking.return_month, booking.return_day_of_month, booking.return_week_of_year, booking.return_day_of_week, booking.return_day_of_week_v2, booking.return_time_bucket, booking.advance_category_day, booking.advance_category_week, booking.advance_category_month, booking.advance_category_date_within_week, booking.advance_pickup_booking_date_diff, booking.comparison_28_days, booking.comparison_period, booking.comparison_common_date, booking.Current_28_Days, booking.4_Weeks_Prior, booking.52_Weeks_Prior, booking.status, booking.booking_type, booking.marketplace_or_dispatch, booking.marketplace_partner, booking.marketplace_partner_summary, booking.booking_channel, booking.booking_source, booking.repeated_user, booking.customer_driving_country, booking.customer_doc_vertification_status, booking.days, booking.extension_days, booking.extra_day_calc, booking.customer_rate, booking.insurance_rate, booking.additional_driver_rate, booking.pai_rate, booking.baby_seat_rate, booking.insurance_type, booking.millage_rate, booking.millage_cap_km, booking.rent_charge, booking.rent_charge_less_discount_extension_aed, booking.extra_day_charge, booking.delivery_charge, booking.collection_charge, booking.additional_driver_charge, booking.insurance_charge, booking.pai_charge, booking.baby_charge, booking.long_distance, booking.premium_delivery, booking.airport_delivery, booking.gps_charge, booking.delivery_update, booking.intercity_charge, booking.millage_charge, booking.other_rental_charge, booking.discount_charge, booking.discount_extension_charge, booking.total_vat, booking.other_charge, booking.booking_charge, booking.booking_charge_less_discount, booking.booking_charge_aed, booking.booking_charge_less_discount_aed, booking.booking_charge_less_extension, booking.booking_charge_less_discount_extension, booking.booking_charge_less_extension_aed, booking.booking_charge_less_discount_extension_aed, booking.base_rental_revenue, booking.non_rental_charge, booking.extension_charge, booking.extension_charge_aed, booking.is_extended, booking.promo_code, booking.promo_code_discount_amount, booking.promocode_created_date, booking.promo_code_description, booking.car_avail_id, booking.car_cat_id, booking.car_cat_name, booking.requested_car, booking.car_name, booking.make, booking.color, booking.deliver_country, booking.deliver_city, booking.country_id, booking.city_id, booking.delivery_location, booking.deliver_method, booking.delivery_lat, booking.delivery_lng, booking.collection_location, booking.collection_method, booking.collection_lat, booking.collection_lng, booking.nps_score, booking.nps_comment, booking.resident_category
        
    FROM ezhire_user_data.user_data_base AS user
        LEFT JOIN ezhire_booking_data.booking_data AS booking ON booking.customer_id = user.user_ptr_id
    -- WHERE
    -- 	DATE_FORMAT(user.date_join_gst, '%Y-%m-%d') = '2024-01-01'
    -- LIMIT 1000
`;

// DROP INDEX
const drop_idx_user_ptr_id_status = `
    DROP INDEX idx_user_ptr_id_status ON ezhire_user_data.user_data_combined_booking_data;
`;

const drop_idx_user_ptr_id_return_date = `
    DROP INDEX idx_user_ptr_id_return_date ON ezhire_user_data.user_data_combined_booking_data;
`;

const drop_idx_user_ptr_id_dates = `
    DROP INDEX idx_user_ptr_id_dates ON ezhire_user_data.user_data_combined_booking_data;
`;

// CREATE INDEX
const create_idx_user_ptr_id_status = `
    CREATE INDEX idx_user_ptr_id_dates ON ezhire_user_data.user_data_combined_booking_data (user_ptr_id, booking_date, pickup_date, return_date);
`;

const create_idx_user_ptr_id_return_date = `
    CREATE INDEX idx_user_ptr_id_status ON ezhire_user_data.user_data_combined_booking_data (user_ptr_id, status);
`;

const create_idx_user_ptr_id_dates = `
    CREATE INDEX idx_user_ptr_id_return_date ON ezhire_user_data.user_data_combined_booking_data (user_ptr_id, return_date);
`;

module.exports = { 
    query_combine_user_and_booking_data,
    drop_idx_user_ptr_id_dates,
    drop_idx_user_ptr_id_return_date,
    drop_idx_user_ptr_id_status,
    create_idx_user_ptr_id_dates,
    create_idx_user_ptr_id_return_date,
    create_idx_user_ptr_id_status,
 };