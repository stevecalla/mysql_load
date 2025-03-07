const schema_booking_table = `
  CREATE TABLE booking_data (
    booking_id INT NOT NULL DEFAULT 0,
    agreement_number VARCHAR(30),

    booking_date DATE,
    booking_datetime DATETIME,

    max_booking_datetime DATETIME,
    today VARCHAR(3),

    booking_year VARCHAR(4),
    booking_quarter VARCHAR(4),
    booking_month VARCHAR(2),
    booking_day_of_month VARCHAR(2),
    booking_week_of_year VARCHAR(2),
    booking_day_of_week VARCHAR(2),
    booking_day_of_week_v2 VARCHAR(64),
    booking_time_bucket VARCHAR(7),

    booking_count BIGINT,
    booking_count_excluding_cancel BIGINT,

    pickup_date DATE,
    pickup_datetime DATETIME,
    pickup_year VARCHAR(4),
    pickup_quarter VARCHAR(4),
    pickup_month VARCHAR(64),
    pickup_day_of_month VARCHAR(2),
    pickup_week_of_year VARCHAR(2),
    pickup_day_of_week VARCHAR(2),
    pickup_day_of_week_v2 VARCHAR(64),
    pickup_time_bucket VARCHAR(7),

    early_return INT,
    return_date DATE,
    return_datetime DATETIME,
    return_year VARCHAR(4),
    return_quarter VARCHAR(4),
    return_month VARCHAR(64),
    return_day_of_month VARCHAR(2),
    return_week_of_year VARCHAR(2),
    return_day_of_week VARCHAR(2),
    return_day_of_week_v2 VARCHAR(64),
    return_time_bucket VARCHAR(7),

    advance_category_day VARCHAR(64),
    advance_category_week VARCHAR(64),
    advance_category_month VARCHAR(64),
    advance_category_date_within_week VARCHAR(64),
    advance_pickup_booking_date_diff BIGINT,

    comparison_28_days VARCHAR(64),
    comparison_period VARCHAR(64),
    comparison_common_date DATE,

    Current_28_Days BIGINT,
    4_Weeks_Prior BIGINT,
    52_Weeks_Prior BIGINT,

    status VARCHAR(50),
    booking_type VARCHAR(12) NOT NULL,
    marketplace_or_dispatch VARCHAR(11) NOT NULL,
    marketplace_partner VARCHAR(100),
    marketplace_partner_summary VARCHAR(100),
    booking_channel VARCHAR(15),
    booking_source VARCHAR(100),

    repeated_user VARCHAR(64) NOT NULL,
    total_lifetime_booking_revenue CHAR(64) NOT NULL,
    no_of_bookings BIGINT NOT NULL DEFAULT 0,
    no_of_cancel_bookings BIGINT,
    no_of_completed_bookings BIGINT,
    no_of_started_bookings BIGINT,
    customer_id INT,
        
    first_name VARCHAR(150),
    last_name VARCHAR(150),
    email VARCHAR(254),

    date_of_birth VARCHAR(25) NOT NULL,
    age BIGINT,

    date_join_formatted_gst DATE,
    date_join_cohort VARCHAR(15),
    date_join_year VARCHAR(4),
    date_join_month VARCHAR(2),
    
    resident_category VARCHAR(50),

    customer_driving_country VARCHAR(50),
    customer_doc_vertification_status VARCHAR(3) NOT NULL,

    days DOUBLE,
    extension_days DOUBLE,
    
    extra_day_calc DOUBLE DEFAULT 0,
    customer_rate DOUBLE,
    insurance_rate DOUBLE,

    additional_driver_rate DOUBLE,
    pai_rate DOUBLE,
    baby_seat_rate DOUBLE,

    insurance_type VARCHAR(14) NOT NULL,
    millage_rate DOUBLE,
    millage_cap_km VARCHAR(15),

    rent_charge DOUBLE,
    rent_charge_less_discount_extension_aed DOUBLE,

    extra_day_charge DOUBLE,
    delivery_charge DOUBLE,
    collection_charge DOUBLE,
    additional_driver_charge DOUBLE,
    insurance_charge DOUBLE,
    
    pai_charge DOUBLE,
    baby_charge DOUBLE,
    long_distance DOUBLE,
    premium_delivery DOUBLE,
    airport_delivery DOUBLE,
    gps_charge DOUBLE,
    delivery_update DOUBLE,

    intercity_charge DOUBLE,
    millage_charge INT NOT NULL DEFAULT 0,
    other_rental_charge DOUBLE,

    discount_charge DOUBLE,
    discount_charge_aed DOUBLE,
    discount_extension_charge DOUBLE,

    total_vat DOUBLE,
    other_charge DOUBLE,

    booking_charge DOUBLE,
    booking_charge_less_discount DOUBLE,
    booking_charge_aed DOUBLE,
    booking_charge_less_discount_aed DOUBLE,

    booking_charge_less_extension DOUBLE,
    booking_charge_less_discount_extension DOUBLE,
    booking_charge_less_extension_aed DOUBLE,
    booking_charge_less_discount_extension_aed DOUBLE,

    base_rental_revenue DOUBLE,
    non_rental_charge DOUBLE,

    extension_charge DOUBLE,
    extension_charge_aed DOUBLE,
    is_extended VARCHAR(3),

    promo_code VARCHAR(200),
    promo_code_discount_amount CHAR(0) NOT NULL,
    promocode_created_date DATETIME,
    promo_code_description VARCHAR(200),
    promo_code_department VARCHAR(50),
    promo_code_expiration_date DATE, 

    car_avail_id INT,
    car_cat_id INT,
    car_cat_name VARCHAR(50),
    requested_car VARCHAR(50),
    car_name VARCHAR(50),
    make VARCHAR(30),
    color VARCHAR(20),

    deliver_country VARCHAR(50) NOT NULL,
    deliver_city VARCHAR(50) NOT NULL,
    country_id INT,
    city_id INT,
    delivery_location VARCHAR(200),
    deliver_method VARCHAR(8) NOT NULL,
    delivery_lat VARCHAR(200),
    delivery_lng VARCHAR(200),
    collection_location VARCHAR(200),
    collection_method VARCHAR(10) NOT NULL,
    collection_lat VARCHAR(200),
    collection_lng VARCHAR(200),
    nps_score VARCHAR(4),
    nps_comment TEXT
  );
`;

module.exports = { schema_booking_table };