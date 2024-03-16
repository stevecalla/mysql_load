//record only exists for 22919
const queryAllPayments = `
    SELECT * 
    FROM all_payments_all
    WHERE Booking_ID IN ('22919', '240797', '240842', '240854', '240872')
    LIMIT 10;
`;

module.exports = { queryAllPayments };
