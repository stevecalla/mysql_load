const schema_user_data_base_table = `
  CREATE TABLE user_data_base (
    -- USER FIELDS FROM auth_user
    auth_user_id INT NOT NULL DEFAULT 0,

    -- first_name VARCHAR(200),
    first_name VARCHAR(200) CHARACTER SET utf8 COLLATE utf8_unicode_ci,
    -- last_name VARCHAR(200),
    last_name VARCHAR(200) CHARACTER SET utf8 COLLATE utf8_unicode_ci,

    email VARCHAR(200),
    last_login_gst DATETIME,
    is_staff INT,
    is_active INT,
    
    -- USER FIELDS FROM rental_fuser
    user_ptr_id INT NOT NULL DEFAULT 0,

    date_join_gst DATETIME,
    date_join_formatted_gst DATE,
    date_join_cohort VARCHAR(10),
    date_join_year INT,
    date_join_month INT,

    is_verified INT,
    date_of_birth DATE,
    is_resident VARCHAR(200),
    renting_in INT,
    
    country_code VARCHAR(200),
    mobile VARCHAR(200),
    telephone VARCHAR(200),
    
    role_type INT,
    
    address_city VARCHAR(200),
    address_country VARCHAR(200),
    
    dl_country VARCHAR(200),
    dl_exp_date DATE,
    int_dl_exp_date DATE,
    passport_exp_date DATE,
    state VARCHAR(200),
    
    payment_det_added INT,
    payment_det_added_bank INT,
    
    user_source1 VARCHAR(200),
    app_version VARCHAR(200),
    os_version VARCHAR(200),
    app_language INT,
    
    gps_added INT,
    insurance_added INT,
    babe_seater_added INT,
    boster_seat_added INT,
    
    firebase_token TEXT,
    has_firebase_token VARCHAR(200),
    
    social_uid VARCHAR(200),
    has_social_uid VARCHAR(200),
    
    user_status INT,
    is_online INT,
    
    referral_code VARCHAR(200),
    referrer_id INT
  );
`;

module.exports = { schema_user_data_base_table };
