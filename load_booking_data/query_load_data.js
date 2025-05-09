function createLoadBookingDataQuery(filePath) {
  return `
  LOAD DATA INFILE '${filePath}'
  INTO TABLE booking_data
  FIELDS TERMINATED BY ','
  ENCLOSED BY '"'
  LINES TERMINATED BY '\\n'
  IGNORE 1 LINES
  (
      booking_id, 
      agreement_number,

      @booking_date,
      @booking_datetime, -- Variable to capture booking_datetime as string

      @max_booking_datetime,
      today,

      booking_year,
      booking_quarter,
      booking_month,
      booking_day_of_month,
      booking_week_of_year,
      booking_day_of_week,
      booking_day_of_week_v2,
      booking_time_bucket,
      
      booking_count,
      booking_count_excluding_cancel,           

      @pickup_date,
      @pickup_datetime, -- Variable to capture pickup_datetime as string
      pickup_year,
      pickup_quarter,
      pickup_month,
      pickup_day_of_month,
      pickup_week_of_year,
      pickup_day_of_week,
      pickup_day_of_week_v2,
      pickup_time_bucket,

      early_return,
      @return_date,
      @return_datetime, -- Variable to capture return_datetime as string
      return_year,
      return_quarter,
      return_month,
      return_day_of_month,
      return_week_of_year,
      return_day_of_week,
      return_day_of_week_v2,
      return_time_bucket,

      advance_category_day,
      advance_category_week,
      advance_category_month,
      advance_category_date_within_week,
      advance_pickup_booking_date_diff,

      comparison_28_days,
      comparison_period,
      @comparison_common_date,

      Current_28_Days,
      4_Weeks_Prior,
      52_Weeks_Prior,

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
          
      first_name,
      last_name,
      email,

      date_of_birth,
      age,
    
      date_join_formatted_gst,
      date_join_cohort,
      date_join_year,
      date_join_month,

      resident_category,
      
      customer_driving_country,
      customer_doc_vertification_status,

      days,
      extension_days,
      
      extra_day_calc,
      customer_rate,
      insurance_rate,

      additional_driver_rate,
      pai_rate,
      baby_seat_rate,

      insurance_type,
      millage_rate,
      millage_cap_km,

      rent_charge,
      rent_charge_less_discount_extension_aed,

      extra_day_charge,
      delivery_charge,
      collection_charge,
      additional_driver_charge,
      insurance_charge,

      pai_charge,
      baby_charge,
      long_distance,
      premium_delivery,
      airport_delivery,
      gps_charge,
      delivery_update,

      intercity_charge,
      millage_charge,
      other_rental_charge,

      discount_charge,
      discount_charge_aed,
      discount_extension_charge,

      total_vat,
      other_charge,

      booking_charge,
      booking_charge_less_discount,
      booking_charge_aed,
      booking_charge_less_discount_aed,
      
      booking_charge_less_extension,
      booking_charge_less_discount_extension,
      booking_charge_less_extension_aed,
      booking_charge_less_discount_extension_aed,

      base_rental_revenue,
      non_rental_charge,

      extension_charge,
      extension_charge_aed,
      is_extended,

      promo_code,
      promo_code_discount_amount,
      @promocode_created_date,  -- Variable to capture promocode_created_date as string
      promo_code_description,
      promo_code_department,
      @promo_code_expiration_date, 
      
      car_avail_id,
      car_cat_id,
      car_cat_name,
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
  SET 
      booking_date = STR_TO_DATE(@booking_date, "%Y-%m-%d"),
      booking_datetime = STR_TO_DATE(@booking_datetime, "%Y-%m-%d %H:%i:%s"),
      max_booking_datetime = STR_TO_DATE(@max_booking_datetime, "%Y-%m-%d %H:%i:%s"),
      pickup_date = STR_TO_DATE(@pickup_date, "%Y-%m-%d"),
      pickup_datetime = STR_TO_DATE(@pickup_datetime, "%Y-%m-%d %H:%i:%s"),
      return_date = STR_TO_DATE(@return_date, "%Y-%m-%d"),
      return_datetime = STR_TO_DATE(@return_datetime, "%Y-%m-%d %H:%i:%s"),
      comparison_common_date = STR_TO_DATE(@comparison_common_date, "%Y-%m-%d"),
      promocode_created_date = STR_TO_DATE(@promocode_created_date, "%Y-%m-%d %H:%i:%s"),
      promo_code_expiration_date = STR_TO_DATE(@promo_code_expiration_date, '%m/%d/%Y')
      ;
`
}

module.exports = { 
  createLoadBookingDataQuery,
};