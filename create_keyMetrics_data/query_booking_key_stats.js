const queryBookingKeyStats = `
    SELECT 
        COALESCE(booking_year, 'Total') AS booking_year,
        COALESCE(booking_month, 'Total') AS booking_month,
        FORMAT(COUNT(booking_datetime), 0) AS Count,
        FORMAT(COUNT(booking_datetime) - COUNT(CASE WHEN status = 'Cancelled by User' THEN 1 END), 0) AS 'Count xCancel',
        CONCAT('$', FORMAT(SUM(booking_charge), 0)) AS booking_charge,
        CONCAT('$', FORMAT(SUM(booking_charge_less_discount), 0)) AS booking_charge_less_discount,
        CONCAT('$', FORMAT(IFNULL(SUM(booking_charge_less_discount) / NULLIF((COUNT(booking_datetime) - COUNT(CASE WHEN status = 'Cancelled by User' THEN 1 END)), 0), 0), 0)) AS 'Charge xCancel Ratio'
    FROM ezhire_booking_data.booking_data
    GROUP BY booking_year, booking_month WITH ROLLUP;
`;

module.exports = { queryBookingKeyStats };