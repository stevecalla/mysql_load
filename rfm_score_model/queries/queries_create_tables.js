const db_name = `mock_rfm_db`;

const query_create_rfm_data_table = `
    CREATE TABLE IF NOT EXISTS rfm_data (
        id INT AUTO_INCREMENT PRIMARY KEY,
        customer_id VARCHAR(255),
        date_of_order DATE,
        order_value INT,
        days_since_last_order INT
    );
`;

const tables_library = [
    { table_name: "rfm_data",
      create_query: query_create_rfm_data_table,
      step: "STEP #2.1:",
      step_info: "rfm_data",
    },
]

module.exports = {
    query_create_rfm_data_table,
    tables_library,
}