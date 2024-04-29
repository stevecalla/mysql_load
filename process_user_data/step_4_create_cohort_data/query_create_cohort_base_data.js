const query_create_cohort_base_data = `
-- Step 1: Create the table structure

CREATE TABLE IF NOT EXISTS user_data_cohort_base (
    id INT PRIMARY KEY AUTO_INCREMENT,

    -- USER DATA FIELDS
    user_ptr_id INT,
    date_join_formatted_gst DATE,
    date_join_cohort VARCHAR(10),

    -- BOOKING DATA FIELDS
    booking_id INT,
    status VARCHAR(64),
    booking_type VARCHAR(64),
    vendor VARCHAR(64),
    is_repeat VARCHAR(64),
    country VARCHAR(64),

    -- id INT PRIMARY KEY AUTO_INCREMENT,
    -- booking_id INT,
    -- status VARCHAR(64),
    -- booking_type VARCHAR(64),
    -- vendor VARCHAR(64),
    -- is_repeat VARCHAR(64),
    -- country VARCHAR(64),

    -- BOOKING DATE FIELD
    booking_date DATE,
    max_booking_datetime DATETIME,

    -- PICKUP DATE FIELDS
    pickup_date DATE,
    pickup_datetime DATETIME,
    pickup_time TIME AS (TIME(pickup_datetime)),

    return_date DATE,
    return_datetime DATETIME,
    return_time TIME AS (TIME(return_datetime)),

    -- CONSTANT MINUTES IN A DAY
    total_minutes_in_day INT DEFAULT (24 * 60),

    -- DAYS CALCULATION
    minutes_rented DECIMAL(20, 4) AS (TIMESTAMPDIFF(MINUTE, pickup_datetime, return_datetime)),
    days_rented DECIMAL(10, 4) AS (TIMESTAMPDIFF(DAY, pickup_datetime, return_datetime)),
    days_less_extension_days DECIMAL(10, 4) AS ((TIMESTAMPDIFF(DAY, pickup_datetime, return_datetime)) - extension_days),
    extension_days DECIMAL(10, 4),

    -- REVENUE CALCULATION
    booking_charge_aed DOUBLE,
    booking_charge_less_discount_aed DOUBLE,

    booking_charge_less_discount_extension_aed DOUBLE,
    extension_charge_aed DOUBLE,

    -- BOOKING CHARGE AED PER DAY
    booking_charge_aed_per_day DOUBLE AS (
        CASE
            WHEN pickup_date = return_date THEN booking_charge_aed
            WHEN pickup_date <> return_date AND days_rented <= 2 THEN booking_charge_aed / 2
            -- ELSE booking_charge_aed / days_rented
            
            WHEN days_rented > 0 THEN booking_charge_aed / days_rented
            ELSE 0            
        END
    ),

    -- BOOKING CHARGE LESS DISCOUNT AED PER DAY
    booking_charge_less_discount_aed_per_day DOUBLE AS (
        CASE
            WHEN pickup_date = return_date THEN booking_charge_less_discount_aed
            WHEN pickup_date <> return_date AND days_rented <= 2 THEN booking_charge_less_discount_aed / 2      
            WHEN days_rented > 0 THEN booking_charge_less_discount_aed / days_rented
            ELSE 0
        END
    ),

    -- BOOKING CHARGE LESS DISCOUNT LESS EXTENSION AED PER DAY
    booking_charge_less_discount_extension_aed_per_day DOUBLE AS (
        CASE
            WHEN pickup_date = return_date THEN booking_charge_less_discount_extension_aed
            WHEN pickup_date <> return_date AND days_less_extension_days <= 2 THEN booking_charge_less_discount_extension_aed / 2
            WHEN days_less_extension_days > 0 THEN booking_charge_less_discount_extension_aed / days_less_extension_days
            ELSE 0
        END
    ),

    -- EXTENSION CHARGE AED PER DAY CALC
    extension_charge_aed_per_day DOUBLE AS (
        CASE
            WHEN pickup_date = return_date THEN extension_charge_aed
            WHEN pickup_date <> return_date AND extension_days <= 2 THEN extension_charge_aed / 2
            WHEN extension_days > 0 THEN extension_charge_aed / extension_days
            ELSE 0
        END
    ),

    -- PICKUP MINUTE FRACTION CALC
    pickup_hours_to_midnight INT AS (HOUR(TIMEDIFF('24:00:00', pickup_time))) VIRTUAL,
    pickup_minutes_to_midnight INT AS (MINUTE(TIMEDIFF('24:00:00', pickup_time))) VIRTUAL,
    pickup_total_minutes_to_midnight INT AS (pickup_hours_to_midnight * 60 + pickup_minutes_to_midnight) VIRTUAL,
    pickup_fraction_of_day DECIMAL(5, 4) AS (pickup_total_minutes_to_midnight / total_minutes_in_day ) STORED,
    
    -- RETURN MINUTE FRACTION CALC
    return_hours_to_midnight INT AS (HOUR(return_time)) VIRTUAL,
    return_minutes_to_midnight INT AS (MINUTE(return_time)) VIRTUAL,
    return_total_minutes_to_midnight INT AS (return_hours_to_midnight * 60 + return_minutes_to_midnight) VIRTUAL,
    return_fraction_of_day DECIMAL(5, 4) AS (return_total_minutes_to_midnight / total_minutes_in_day ) STORED,

    -- Create indexes on pickup_date, return_date, and status in key_metrics_base
    INDEX idx_pickup_date (pickup_date),
    INDEX idx_return_date (return_date),
    INDEX idx_status (status),
    INDEX idx_pickup_return_date (pickup_date, return_date)
    );
`;

module.exports = { 
    query_create_cohort_base_data 
};