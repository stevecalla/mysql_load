const queryAllPayments = `
    SELECT * 
    FROM all_payments_all
    WHERE Booking_ID IN ('240733', '240797', '240842', '240854', '240872')
    LIMIT 1;
`;

module.exports = { queryAllPayments };
