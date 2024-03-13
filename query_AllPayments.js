const queryAllPayments = `
    SELECT * 
    FROM all_payments_all 
    LIMIT 1;
`;

module.exports = { queryAllPayments };
