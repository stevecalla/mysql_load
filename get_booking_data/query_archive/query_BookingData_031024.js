const queryBookingData = `
select 
 booking_id
, REPLACE(REPLACE(agreement_number, '"', ''), ',', ' ') AS agreement_number -- eliminate extra double quotes within the text string replace the double quotes (") with an empty string ('')
, IFNULL(IF(DATE_FORMAT(booking_datetime, '%Y-%m-%d %H:%i:%s') = '0000-00-00 00:00:00', '1900-01-01 12:00:00', booking_datetime), '1900-01-01 12:00:00') AS booking_datetime
-- , IFNULL(CAST(booking_datetime AS CHAR), '1900-01-01 12:00:00') AS booking_datetime
-- ,booking_datetime_v2
,booking_year
,booking_month
,booking_day_of_month
,booking_day_of_week
,booking_day_of_week_v2
,booking_time_bucket
, IFNULL(IF(DATE_FORMAT(pickup_datetime, '%Y-%m-%d %H:%i:%s') = '0000-00-00 00:00:00', '1900-01-01 12:00:00', pickup_datetime), '1900-01-01 12:00:00') AS pickup_datetime
-- ,pickup_datetime_v2
,pickup_year
,pickup_month
,pickup_day_of_month
,pickup_day_of_week
,pickup_day_of_week_v2
,pickup_time_bucket
, IFNULL(IF(DATE_FORMAT(return_datetime, '%Y-%m-%d %H:%i:%s') = '0000-00-00 00:00:00', '1900-01-01 12:00:00', return_datetime), '1900-01-01 12:00:00') AS return_datetime
-- ,return_datetime_v2
,return_year
,return_month
,return_day_of_month
,return_day_of_week
,return_day_of_week_v2
,return_time_bucket
,status
,booking_type
,marketplace_or_dispatch
,marketplace_partner
,marketplace_partner_summary
,booking_channel
,booking_source
,repeated_user
,total_lifetime_booking_revenue
,no_of_bookings
,no_of_cancel_bookings
,no_of_completed_bookings
,no_of_started_bookings
,customer_id
,date_of_birth
,age
,customer_driving_country
,customer_doc_vertification_status
,days
, IFNULL(extra_day_calc, 0) AS extra_day_calc
, IFNULL(myproject.get_rental_rates(tb.booking_id, tb.millage_id, tb.contract_id, tb.drg, tb.wrg, tb.mrg, tb.dr, tb.wr, tb.mr, tb.days, tb.deliver_date_string), 0) * tb.conversion_rate AS customer_rate
, IFNULL(insurance_rate, 0) AS insurance_rate
, IFNULL(insurance_type, 0) AS insurance_type
, IFNULL(millage_rate, 0) AS millage_rate
, IFNULL(millage_cap_km, 0) AS millage_cap_km
, IFNULL(rent_charge, 0) AS rent_charge
, IFNULL(extra_day_charge, 0) AS extra_day_charge
, IFNULL(delivery_charge, 0) AS delivery_charge
, IFNULL(collection_charge, 0) AS collection_charge
, IFNULL(additional_driver_charge, 0) AS additional_driver_charge
, IFNULL(insurance_charge, 0) AS insurance_charge
, IFNULL(intercity_charge, 0) AS intercity_charge
, IFNULL(millage_charge, 0) AS millage_charge
, IFNULL(other_rental_charge, 0) AS other_rental_charge
, IFNULL(discount_charge, 0) AS discount_charge
, IFNULL(total_vat, 0) AS total_vat
, IFNULL(other_charge, 0) AS other_charge
, IFNULL(booking_charge, 0) AS booking_charge
, IFNULL(booking_charge_less_discount, 0) AS booking_charge_less_discount
, IFNULL(base_rental_revenue, 0) AS base_rental_revenue
, IFNULL(non_rental_charge, 0) AS non_rental_charge
, IFNULL(extension_charge, 0) AS extension_charge
,is_extended
,Promo_Code AS promo_code
,promo_code_discount_amount
, IFNULL(IF(DATE_FORMAT(promocode_created_date, '%Y-%m-%d %H:%i:%s') = '0000-00-00 00:00:00', '1900-01-01 12:00:00', promocode_created_date), '1900-01-01 12:00:00') AS promocode_created_date
,promo_code_description
,requested_car
,car_name
,make
-- ,color
, REPLACE(color, ",", '') AS color
,deliver_country
,deliver_city
, REPLACE(REPLACE(REPLACE(delivery_location, '\n', ''), ',', ''), '"', '') AS delivery_location -- eliminate extra double quotes within the text string replace the double quotes (") with an empty string ('')
,deliver_method
,delivery_lat
,delivery_lng
, REPLACE(REPLACE(REPLACE(collection_location, '\n', ''), ',', ''), '"', '') AS collection_location -- eliminate extra double quotes within the text string replace the double quotes (") with an empty string ('')
,collection_method
-- ,collection_lat
, IFNULL(SUBSTRING_INDEX(collection_lat, ',', 1), collection_lat) AS collection_lat
-- ,collection_lng
, IFNULL(SUBSTRING_INDEX(collection_lng, ',', 1), collection_lat) AS collection_lng
,nps_score
, NULLIF(REPLACE(REPLACE(REPLACE(nps_comment, '\n', ''), ',', ''), '"', ''), '') AS nps_comment
FROM (
SELECT 
b.id AS booking_id,
agreement_number,
b.millage_id, 
b.contract_id,
b.drg,
b.wrg,
b.mrg,
b.dr,
b.wr,
b.mr,
b.deliver_date_string,
date_format(date_add(b.created_on ,interval 4 hour), '%Y-%m-%d %H:%i:%s') AS booking_datetime
-- ,b.created_on AS booking_datetime_v2 -- raw date; need to convert to GMT+4
,date_format(date_add(b.created_on ,interval 4 hour),'%Y') booking_year
,date_format(date_add(b.created_on ,interval 4 hour),'%m') booking_month
,date_format(date_add(b.created_on ,interval 4 hour),'%d') booking_day_of_month
,date_format(date_add(b.created_on ,interval 4 hour),'%u') booking_day_of_week
,date_format(date_add(b.created_on ,interval 4 hour),'%W') booking_day_of_week_v2
,date_format(date_add(b.created_on ,interval 4 hour),'%H') booking_time_bucket
-- ,date_format(CONCAT(STR_TO_DATE(b.deliver_date_string,'%d/%m/%Y'), ' ', b.deliver_time_string), '%W, %M %e, %Y %H:%i') AS pickup_datetime
,date_format(CONCAT(STR_TO_DATE(b.deliver_date_string,'%d/%m/%Y'), ' ', b.deliver_time_string), '%Y-%m-%d %H:%i:%s') AS pickup_datetime -- DOES THIS REQUIRE 4 hour timezone adjustment?
-- ,CONCAT(b.deliver_date_string, ' ', b.deliver_time_string) AS pickup_datetime_v2
,date_format(CONCAT(STR_TO_DATE(b.deliver_date_string,'%d/%m/%Y'), ' ', b.deliver_time_string),'%Y') pickup_year
,date_format(CONCAT(STR_TO_DATE(b.deliver_date_string,'%d/%m/%Y'), ' ', b.deliver_time_string),'%m') pickup_month
,date_format(CONCAT(STR_TO_DATE(b.deliver_date_string,'%d/%m/%Y'), ' ', b.deliver_time_string),'%d') pickup_day_of_month
,date_format(CONCAT(STR_TO_DATE(b.deliver_date_string,'%d/%m/%Y'), ' ', b.deliver_time_string),'%u') pickup_day_of_week
,date_format(CONCAT(STR_TO_DATE(b.deliver_date_string,'%d/%m/%Y'), ' ', b.deliver_time_string),'%W') pickup_day_of_week_v2
,date_format(CONCAT(STR_TO_DATE(b.deliver_date_string,'%d/%m/%Y'), ' ', b.deliver_time_string),'%H') pickup_time_bucket
-- ,date_format(CONCAT(STR_TO_DATE(b.return_date_string,'%d/%m/%Y'), ' ', b.return_time_string), '%W, %M %e, %Y %H:%i') AS return_datetime
,date_format(CONCAT(STR_TO_DATE(b.return_date_string,'%d/%m/%Y'), ' ', b.return_time_string), '%Y-%m-%d %H:%i:%s') AS return_datetime -- DOES THIS REQUIRE 4 hour timezone adjustment?
-- ,CONCAT(b.return_date_string, ' ', b.return_time_string) AS return_datetime_v2
,date_format(CONCAT(STR_TO_DATE(b.return_date_string,'%d/%m/%Y'), ' ', b.return_time_string),'%Y') return_year
,date_format(CONCAT(STR_TO_DATE(b.return_date_string,'%d/%m/%Y'), ' ', b.return_time_string),'%m') return_month
,date_format(CONCAT(STR_TO_DATE(b.return_date_string,'%d/%m/%Y'), ' ', b.return_time_string),'%d') return_day_of_month
,date_format(CONCAT(STR_TO_DATE(b.return_date_string,'%d/%m/%Y'), ' ', b.return_time_string),'%u') return_day_of_week
,date_format(CONCAT(STR_TO_DATE(b.return_date_string,'%d/%m/%Y'), ' ', b.return_time_string),'%W') return_day_of_week_v2
,date_format(CONCAT(STR_TO_DATE(b.return_date_string,'%d/%m/%Y'), ' ', b.return_time_string),'%H') return_time_bucket
,(SELECT status FROM myproject.rental_status rs WHERE rs.id = b.status) AS status,
(CASE 
WHEN b.days < 7 THEN 'daily' 
WHEN b.days > 29 AND is_subscription=1 THEN 'Subscription'
WHEN b.days > 29 THEN 'Monthly'
ELSE 'Weekly' 
END) AS booking_type
,(CASE 
		WHEN b.vendor_id = 234555 THEN 'Dispatch' 
		WHEN b.vendor_id <> 234555 THEN 'MarketPlace'
ELSE 'N/A'
END) AS marketplace_or_dispatch
,(SELECT name FROM myproject.rental_vendors rv WHERE rv.owner_id = b.vendor_id AND b.vendor_id <> 234555) AS marketplace_partner
,(SELECT name FROM myproject.rental_vendors rv WHERE rv.owner_id = b.vendor_id) AS marketplace_partner_summary
,b.platform_generated AS booking_channel
,(select name FROM myproject.rental_car_booking_source bs WHERE bs.id = b.car_booking_source_id) as booking_source
,''total_lifetime_booking_revenue
,(CASE WHEN (SELECT COUNT(1) FROM myproject.rental_car_booking2 bb WHERE bb.owner_id = b.owner_id)> 1 THEN 'YES' ELSE 'NO' END) repeated_user
,(SELECT COUNT(1) FROM myproject.rental_car_booking2 bb WHERE bb.owner_id = b.owner_id) AS no_of_bookings
,(SELECT COUNT(1) FROM myproject.rental_car_booking2 bb WHERE bb.owner_id = b.owner_id AND bb.status = 8) AS no_of_cancel_bookings
,(SELECT COUNT(1) FROM myproject.rental_car_booking2 bb WHERE bb.owner_id = b.owner_id AND bb.status = 9) AS no_of_completed_bookings
,(SELECT COUNT(1) FROM myproject.rental_car_booking2 bb WHERE bb.owner_id = b.owner_id AND bb.status not in (8,9)) AS no_of_started_bookings
,b.owner_id as customer_id
,f.date_of_birth
,TIMESTAMPDIFF(YEAR,str_to_date(f.date_of_birth,'%d/%m/%Y'),now()) age
,(select name FROM myproject.rental_country ct where ct.code = dl_country) customer_driving_country
,(CASE WHEN f.is_verified > 0 THEN 'YES' ELSE 'NO' END) customer_doc_vertification_status
,b.days
,(select SUM(total_charge) FROM myproject.rental_charges cc WHERE cc.booking_id = b.id and cc.charge_type_id in (30,31)) as extra_day_calc
,(CASE WHEN b.days < 7 then b.DIR WHEN b.days > 29 THEN b.MIR ELSE b.WIR END) as insurance_rate
,(case when (CASE WHEN b.days < 7 then b.DIR WHEN b.days > 29 THEN b.MIR ELSE b.WIR END) > 0 then 'Full Insurance' else '' end) insurance_type
,(select ad.rate FROM myproject.cars_available_detail ad where ad.car_available_id = b.car_available_id and ad.millage_id = b.millage_id and ad.month_id = b.contract_id) millage_rate
,(select name FROM myproject.Allowed_Millage am where am.id = b.millage_id) millage_cap_km
,(select SUM(total_charge) FROM myproject.rental_charges cc WHERE cc.booking_id = b.id and cc.charge_type_id = 4) as rent_charge
,(select SUM(total_charge) FROM myproject.rental_charges cc WHERE cc.booking_id = b.id and cc.charge_type_id in (31,30)) as extra_day_charge
,(select SUM(total_charge) FROM myproject.rental_charges cc WHERE cc.booking_id = b.id and cc.charge_type_id = 11) as delivery_charge
,(select SUM(total_charge) FROM myproject.rental_charges cc WHERE cc.booking_id = b.id and cc.charge_type_id = 3) as collection_charge
,(select SUM(total_charge) FROM myproject.rental_charges cc WHERE cc.booking_id = b.id and cc.charge_type_id in (21,40) ) as additional_driver_charge
,(select SUM(total_charge) FROM myproject.rental_charges cc WHERE cc.booking_id = b.id and cc.charge_type_id in (15,36)) as insurance_charge
,(select SUM(total_charge) FROM myproject.rental_charges cc WHERE cc.booking_id = b.id and cc.charge_type_id = 25) as intercity_charge
,0  as millage_charge
,(select SUM(total_charge) FROM myproject.rental_charges cc WHERE cc.booking_id = b.id and cc.charge_type_id  IN (15,16,17,18,19,23,26,29,32,37,38,39,41,48,49,50,51,52,56,57)) as other_rental_charge
,(select SUM(total_charge) FROM myproject.rental_charges cc WHERE cc.booking_id = b.id and cc.charge_type_id = 14) as discount_charge
,(select SUM(total_charge) FROM myproject.rental_charges cc WHERE cc.booking_id = b.id and cc.charge_type_id = 20) as total_vat
,(select SUM(total_charge) FROM myproject.rental_charges cc WHERE cc.booking_id = b.id and cc.charge_type_id in (1,2,8,9,13,14,20,22,24,27,28,44,45,46,47)) as other_charge
,(select SUM(total_charge) FROM myproject.rental_charges cc WHERE cc.booking_id = b.id and cc.charge_type_id in (3,4,11,15,16,17,18,19,21,23,25,26,29,30,31,32,36,37,38,39,40,41,48,49,50,51,52,56,57)) as booking_charge
,(select SUM(CASE WHEN charge_type_id in (14) THEN -(total_charge) ELSE (total_charge) END)  FROM myproject.rental_charges cc WHERE cc.booking_id = b.id and cc.charge_type_id in (3,4,11,15,16,17,18,19,21,23,25,26,29,30,31,32,36,37,38,39,40,41,48,49,50,51,52,56,57,14)) as booking_charge_less_discount
,(select SUM(total_charge) FROM myproject.rental_charges cc WHERE cc.booking_id = b.id and cc.charge_type_id in (3,4,11,15,16,17,18,19,21,23,25,26,29,30,31,32,36,37,38,39,40,41,48,49,50,51,52,56,57)) as base_rental_revenue
,(select SUM(total_charge) FROM myproject.rental_charges cc WHERE cc.booking_id = b.id and cc.charge_type_id in  (1,2,8,9,13,14,20,22,24,27,28,44,45,46,47)) as non_rental_charge
,0  as extension_charge
,(SELECT CASE WHEN COUNT(1) >= 1 THEN 'YES' ELSE 'NO' END FROM myproject.rental_invoice_details WHERE type = 'Extension' AND booking_id = b.id) AS is_extended
,pc.Promo_Code
,'' promo_code_discount_amount
,date_format(pc.date_created, '%Y-%m-%d %H:%i:%s') promocode_created_date
-- ,date_format(pc.date_created, '%W, %M %e, %Y %H:%i') promocode_created_date
,b.Promo_Code promo_code_description
,ca.car_name requested_car
,c.car_name
,c.make
,c.color
,co.name deliver_country
,rc.name deliver_city
,b.delivery_location
,(case when b.self_pickup_status = 1 then 'Self' else 'Delivery' end) deliver_method
,b.delivery_location_lat delivery_lat
,b.delivery_location_lng delivery_lng
,b.collection_location
,(case when b.self_return_status = 1 then 'Self' else 'Collection' end)  collection_method
,b.return_location_lat collection_lat
,b.return_location_lng collection_lng
,(SELECT rate FROM myproject.rental_rentalfeedback rf WHERE rf.booking_id = b.id order by rf.id desc limit 1)  nps_score
,(SELECT ct.conversion_rate FROM myproject.country_conversion_rate ct, myproject.rental_city c WHERE ct.country_id = c.CountryID AND c.id = b.city_id) AS conversion_rate
,(SELECT comments FROM myproject.rental_rentalfeedback rf WHERE rf.booking_id = b.id order by rf.id desc limit 1) nps_comment
FROM myproject.rental_car_booking2 b
INNER JOIN myproject.rental_fuser f ON f.user_ptr_id = b.owner_id
INNER JOIN myproject.rental_city rc on rc.id = b.city_id
INNER JOIN myproject.rental_country co on co.id = rc.CountryID
LEFT JOIN myproject.rental_vendors rv on rv.owner_id = b.vendor_id 
LEFT JOIN myproject. rental_car c ON c.id = b.car_id
LEFT JOIN myproject.rental_cars_available ca on ca.id = b.car_available_id
LEFT JOIN myproject.rental_add_promo_codes pc ON pc.id = b.Promo_Code_id
-- WHERE date(date_add(b.created_on,interval 4 hour)) between '2024-01-01' and '2024-01-01' AND pc.Promo_Code IS NOT NULL
WHERE date(date_add(b.created_on,interval 4 hour)) between 'startDateVariable' and 'endDateVariable'
-- AND b.id = "211954"
ORDER BY b.id 
-- LIMIT 1
)tb;
`;

module.exports = { queryBookingData };