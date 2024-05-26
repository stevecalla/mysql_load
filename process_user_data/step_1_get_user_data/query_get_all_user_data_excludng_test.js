const queryUserData = `
    -- GET USERS DATA EXCLUDING TEST USERS
    SELECT
        -- USER FIELDS FROM auth_user
        auth_user.id AS auth_user_id,
        -- REPLACE(auth_user.first_name, ',', '') AS first_name,
        CASE
            WHEN auth_user.first_name LIKE '%בסד הנדסה%' THEN 'Contains specific Arabic characters'
            WHEN auth_user.first_name LIKE "%Fx %" THEN ''
            WHEN auth_user.first_name LIKE "%,%" THEN REPLACE(auth_user.first_name, ',', '')
            ELSE auth_user.first_name
        END AS first_name,
    
        -- REPLACE(auth_user.last_name, ',', '') AS last_name,
        CASE
            WHEN auth_user.last_name LIKE '%اف اكس .%' THEN REPLACE(auth_user.last_name, '.', '')
            WHEN auth_user.last_name LIKE "%Fx %" THEN ''
            WHEN auth_user.last_name LIKE "%,%" THEN REPLACE(auth_user.last_name, ',', '')
            ELSE auth_user.last_name
        END AS last_name,
    
        -- auth_user.email,
        CASE 
            WHEN LOCATE('\\n', auth_user.email) > 0 THEN REPLACE(auth_user.email, '\\n', '') -- locate & remove \n
            ELSE auth_user.email
        END AS email,

        DATE_FORMAT(DATE_ADD(auth_user.last_login, INTERVAL 4 HOUR), '%Y-%m-%d %H:%i:%s') AS last_login_gst,
        auth_user.is_staff,
        auth_user.is_active,

        -- USER FIELDS FROM rental_fuser
        fuser.user_ptr_id,

        DATE_FORMAT(DATE_ADD(fuser.date_join, INTERVAL 4 HOUR), '%Y-%m-%d %H:%i:%s') AS date_join_gst,
        DATE_FORMAT(DATE_ADD(fuser.date_join, INTERVAL 4 HOUR), '%Y-%m-%d') AS date_join_formatted_gst,
        DATE_FORMAT(DATE_ADD(fuser.date_join, INTERVAL 4 HOUR), '%Y-%m') AS date_join_cohort,
        DATE_FORMAT(DATE_ADD(fuser.date_join, INTERVAL 4 HOUR), '%Y') AS date_join_year,
        DATE_FORMAT(DATE_ADD(fuser.date_join, INTERVAL 4 HOUR), '%m') AS date_join_month,

        fuser.is_verified,
        -- fuser.date_of_birth,
        CASE
            WHEN fuser.date_of_birth = '20-11' THEN '1900-01-01'
            WHEN STR_TO_DATE(fuser.date_of_birth, '%d/%m/%Y') THEN DATE_FORMAT(STR_TO_DATE(fuser.date_of_birth, '%d/%m/%Y'), '%Y-%m-%d')
            WHEN STR_TO_DATE(fuser.date_of_birth, '%m/%d/%Y') THEN DATE_FORMAT(STR_TO_DATE(fuser.date_of_birth, '%m/%d/%Y'), '%Y-%m-%d')
            WHEN STR_TO_DATE(fuser.date_of_birth,  '%Y-%m-%d %H:%i:%s') THEN DATE_FORMAT(STR_TO_DATE(fuser.date_of_birth,  '%Y-%m-%d %H:%i:%s'), '%Y-%m-%d')
            ELSE NULL
        END AS date_of_birth,
        fuser.is_resident,
        fuser.renting_in,

        fuser.country_code,
        REPLACE(fuser.mobile, ',', '') AS mobile,
        REPLACE(fuser.telephone, ',', '') AS telephone,

        fuser.role_type,

        fuser.city AS address_city,
        fuser.country AS address_country,

        fuser.dl_country,

        CASE
            WHEN fuser.dl_exp_date = '0000-00-00' THEN '1900-01-01'
            ELSE DATE_FORMAT(fuser.dl_exp_date, '%Y-%m-%d')
        END AS dl_exp_date,
        CASE
            WHEN fuser.int_dl_exp_date = '0000-00-00' THEN '1900-01-01'
            ELSE DATE_FORMAT(fuser.int_dl_exp_date, '%Y-%m-%d')
        END AS int_dl_exp_date,
        CASE
            WHEN fuser.passport_exp_date = '0000-00-00' THEN '1900-01-01'
            ELSE DATE_FORMAT(fuser.passport_exp_date, '%Y-%m-%d')
        END AS passport_exp_date,

        fuser.state,
        fuser.Payment_Det_Added AS payment_det_added,
        IFNULL(fuser.payment_det_added_bank, '') AS payment_det_added_bank,

        fuser.user_source1,
        fuser.app_version,
        fuser.os_version,
        fuser.app_language,

        fuser.Gps_added AS gps_added,
        fuser.Insurance_added AS insurance_added,
        fuser.babe_seater_added,
        fuser.boster_seat_added,
        
        fuser.firebase_token,
        CASE
            WHEN fuser.firebase_token IS NOT NULL THEN 'yes'
            ELSE 'no'
        END has_firebase_token,
        fuser.social_uid,
        CASE
            WHEN fuser.social_uid IS NOT NULL THEN 'yes'
            ELSE 'no'
        END has_social_uid,

        fuser.user_status,
        fuser.is_online,

        REPLACE(fuser.referral_code, ',', '') AS referral_code,
        referrer_id

    FROM
        rental_fuser AS fuser
        LEFT JOIN myproject.auth_user AS auth_user ON auth_user.id = fuser.user_ptr_id
    WHERE
        -- DATE FILTER 
        -- DATE(DATE_ADD(fuser.date_join, INTERVAL 4 HOUR)) BETWEEN @str_date AND @end_date
        
        -- USE IN JS SQL BECAUSE SET DOESN'T WORK
        -- DATE(DATE_ADD(fuser.date_join, INTERVAL 4 HOUR)) BETWEEN '2024-01-01' AND '2024-01-01'
        -- AND

        -- auth_user.id IN ('109711', '232623', '495776') -- first or last name contains arabic characters & errors

        -- auth_user.id IN ( '232623', '495776') -- first or last name contains arabic characters & errors
        -- auth_user.id IN ('364173') -- bad date = 0000-00-00 & errors
        -- -- auth_user.id IN ('97967') -- bad date of birth 2020-11-00
        -- auth_user.id IN ('476528', '478349') -- email address includes \n

        -- AND
        -- LOGIC EXCLUDE TEST USERS FROM auth_user
        LOWER(auth_user.first_name) NOT LIKE '%test%'
        AND LOWER(auth_user.last_name) NOT LIKE '%test%'
        AND LOWER(auth_user.username) NOT LIKE '%test%'
        AND LOWER(auth_user.email) NOT LIKE '%test%'
        AND auth_user.last_name NOT LIKE 'N'
        AND auth_user.email NOT LIKE 'abc@gmail.com'
        
        -- LOGIC TO EXCLUDE TEST BOOKINGS
        -- AND COALESCE(rcb.vendor_id,'') NOT IN (5, 33, 218, 23086)

    GROUP BY fuser.user_ptr_id
    -- LIMIT 100;
`;

module.exports = { queryUserData };