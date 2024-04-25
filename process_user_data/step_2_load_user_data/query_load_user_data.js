function load_user_data_query(filePath) {
  return `
  LOAD DATA INFILE '${filePath}'
  INTO TABLE user_data
  FIELDS TERMINATED BY ','
  ENCLOSED BY '"'
  LINES TERMINATED BY '\\n'
  IGNORE 1 LINES
  (
    -- USER FIELDS FROM auth_user
    auth_user_id,
    first_name,
    last_name,
    email,
    @last_login_gst,
    is_staff,
    is_active,
    
    -- USER FIELDS FROM rental_fuser
    user_ptr_id,
    @date_join_gst,
    is_verified,
    -- date_of_birth,
    @date_of_birth,
    is_resident,
    renting_in,
    
    country_code,
    mobile,
    telephone,
    
    role_type,
    
    address_city,
    address_country,
    
    dl_country,

    @dl_exp_date,
    @int_dl_exp_date,
    @passport_exp_date,
    state,
    
    payment_det_added,
    payment_det_added_bank,
    
    user_source1,
    app_version,
    os_version,
    app_language,
    
    gps_added,
    insurance_added,
    babe_seater_added,
    boster_seat_added,
    
    firebase_token,
    has_firebase_token,
    
    social_uid,
    has_social_uid,
    
    user_status,
    is_online,
    
    referral_code,
    referrer_id
  )
  SET 
    last_login_gst = STR_TO_DATE(@last_login_gst, "%Y-%m-%d %H:%i:%s"),
    date_join_gst = STR_TO_DATE(@date_join_gst, "%Y-%m-%d %H:%i:%s"),
    date_of_birth = STR_TO_DATE(@date_of_birth, "%Y-%m-%d"),
    dl_exp_date = STR_TO_DATE(@dl_exp_date, "%Y-%m-%d"),
    int_dl_exp_date = STR_TO_DATE(@int_dl_exp_date, "%Y-%m-%d"),
    passport_exp_date = STR_TO_DATE(@passport_exp_date, "%Y-%m-%d");
`
}

module.exports = { 
  load_user_data_query
};