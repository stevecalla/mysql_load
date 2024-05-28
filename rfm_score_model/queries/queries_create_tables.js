const query_create_rfm_data_table = `
    CREATE TABLE IF NOT EXISTS rfm_data (
        id INT AUTO_INCREMENT PRIMARY KEY,
        customer_id VARCHAR(255),
        date_of_order DATE,
        order_value INT,
        days_since_last_order INT
    );
`;

function query_create_rfm_data_rollup_table(table_name) {
    return(`
        CREATE TABLE ${table_name}
        SELECT
            customer_id,
            MIN(days_since_last_order) AS days_since_last_order,
            MAX(date_of_order) AS date_of_order,
            COUNT(customer_id) AS total_number_of_orders,
            SUM(order_value) AS total_order_value
        FROM rfm_data
        GROUP BY customer_id
        ORDER BY customer_id;
    `);
}

function query_create_recency_score_table(
    table_name, 
    rfm_value, 
    rfm_label, 
    rfm_greater_or_less,
    rfm_range_start_score_low,
    rfm_range_end_score_low,
    rfm_range_start_score_middle,
    rfm_range_end_score_middle,
) {
    return(`
        CREATE TABLE ${table_name}
        SELECT 
            customer_id, 
            ${rfm_value}, 
            date_of_order,
            FORMAT(percent_rank() OVER (ORDER BY ${rfm_value}), 2) AS ${rfm_label}_rank, -- base rank i.e. 0, 10%, 20%... for each row
            ROW_NUMBER() OVER (ORDER BY ${rfm_value}, date_of_order) AS row_number_id, -- row number
            COUNT(*) OVER () AS total_rows, -- total number of rows
            ROW_NUMBER() OVER (ORDER BY ${rfm_value}, date_of_order) / COUNT(*) OVER () AS row_percent, -- row as a percent of total rows; i.e. row 3 of 10 is 30%
            CASE
                WHEN ROW_NUMBER() OVER (ORDER BY ${rfm_value}, date_of_order) / COUNT(*) OVER () ${rfm_greater_or_less} 0.66 THEN 1
                WHEN ROW_NUMBER() OVER (ORDER BY ${rfm_value}, date_of_order) / COUNT(*) OVER () ${rfm_greater_or_less} 0.33 THEN 2
                ELSE 3
            END AS ${rfm_label}_score_three_parts, -- scoring dividing the data into three equal parts
            CASE
                WHEN ROW_NUMBER() OVER (ORDER BY ${rfm_value}, date_of_order) / COUNT(*) OVER () ${rfm_greater_or_less} 0.80 THEN 1
                WHEN ROW_NUMBER() OVER (ORDER BY ${rfm_value}, date_of_order) / COUNT(*) OVER () ${rfm_greater_or_less} 0.60 THEN 2
                WHEN ROW_NUMBER() OVER (ORDER BY ${rfm_value}, date_of_order) / COUNT(*) OVER () ${rfm_greater_or_less} 0.40 THEN 3
                WHEN ROW_NUMBER() OVER (ORDER BY ${rfm_value}, date_of_order) / COUNT(*) OVER () ${rfm_greater_or_less} 0.20 THEN 4
                ELSE 5
            END AS ${rfm_label}_score_five_parts, -- scoring dividing the data into three equal parts
            CASE
                WHEN ${rfm_value} BETWEEN ${rfm_range_start_score_low} AND ${rfm_range_end_score_low} THEN 3
                WHEN ${rfm_value} BETWEEN ${rfm_range_start_score_middle} AND ${rfm_range_end_score_middle} THEN 2
                ELSE 1
            END AS ${rfm_label}_score_custom_parts -- scoring using custom buckets... to keep similar customers in same bucket
        FROM rfm_data_rollup
        ORDER BY ${rfm_value} ASC, date_of_order DESC;
    `);
}

function query_create_frequency_monetary_score_table(
    table_name, 
    rfm_value, 
    rfm_label, 
    rfm_greater_or_less,
    rfm_range_start_score_low,
    rfm_range_end_score_low,
    rfm_range_start_score_middle,
    rfm_range_end_score_middle,
) {
    return(`
        CREATE TABLE ${table_name}
        SELECT 
            customer_id, 
            ${rfm_value}, 
            date_of_order,
            FORMAT(percent_rank() OVER (ORDER BY ${rfm_value}), 2) AS ${rfm_label}_rank, -- base rank i.e. 0, 10%, 20%... for each row
            ROW_NUMBER() OVER (ORDER BY ${rfm_value}, date_of_order) AS row_number_id, -- row number
            COUNT(*) OVER () AS total_rows, -- total number of rows
            ROW_NUMBER() OVER (ORDER BY ${rfm_value}, date_of_order) / COUNT(*) OVER () AS row_percent, -- row as a percent of total rows; i.e. row 3 of 10 is 30%
            CASE
                WHEN ROW_NUMBER() OVER (ORDER BY ${rfm_value}, date_of_order) / COUNT(*) OVER () ${rfm_greater_or_less} 0.33 THEN 1
                WHEN ROW_NUMBER() OVER (ORDER BY ${rfm_value}, date_of_order) / COUNT(*) OVER () ${rfm_greater_or_less} 0.66 THEN 2
                ELSE 3
            END AS ${rfm_label}_score_three_parts, -- scoring dividing the data into three equal parts
            CASE
                WHEN ROW_NUMBER() OVER (ORDER BY ${rfm_value}, date_of_order) / COUNT(*) OVER () ${rfm_greater_or_less} 0.20 THEN 1
                WHEN ROW_NUMBER() OVER (ORDER BY ${rfm_value}, date_of_order) / COUNT(*) OVER () ${rfm_greater_or_less} 0.40 THEN 2
                WHEN ROW_NUMBER() OVER (ORDER BY ${rfm_value}, date_of_order) / COUNT(*) OVER () ${rfm_greater_or_less} 0.60 THEN 3
                WHEN ROW_NUMBER() OVER (ORDER BY ${rfm_value}, date_of_order) / COUNT(*) OVER () ${rfm_greater_or_less} 0.80 THEN 4
                ELSE 5
            END AS ${rfm_label}_score_five_parts, -- scoring dividing the data into three equal parts
            CASE
                WHEN ${rfm_value} BETWEEN ${rfm_range_start_score_low} AND ${rfm_range_end_score_low} THEN 1
                WHEN ${rfm_value} BETWEEN ${rfm_range_start_score_middle} AND ${rfm_range_end_score_middle} THEN 2
                ELSE 3
            END AS ${rfm_label}_score_custom_parts -- scoring using custom buckets... to keep similar customers in same bucket
        FROM rfm_data_rollup
        ORDER BY ${rfm_value} ASC, date_of_order ASC;
    `);
}

function query_create_rfm_score_summary_table(table_name, rfm_value, rfm_label, rfm_greater_or_less) {
    return(`
        CREATE TABLE ${table_name}
            SELECT
                ru.customer_id,
                ru.date_of_order,
                ru.days_since_last_order,
                ru.total_number_of_orders,
                ru.total_order_value,
                CONCAT(r.recency_score_three_parts, f.frequency_score_three_parts, m.monetary_score_three_parts) AS score_three_parts,
                CONCAT(r.recency_score_five_parts, f.frequency_score_five_parts, m.monetary_score_five_parts) AS score_five_parts,
                CONCAT(r.recency_score_custom_parts, f.frequency_score_custom_parts, m.monetary_score_custom_parts) AS score_custom_parts
            FROM rfm_data_rollup AS ru
            INNER JOIN rfm_score_recency_data AS r ON ru.customer_id = r.customer_id
            INNER JOIN rfm_score_frequency_data AS f ON ru.customer_id = f.customer_id
            INNER JOIN rfm_score_monetary_data AS m ON ru.customer_id = m.customer_id
            ORDER BY r.days_since_last_order DESC;
    `);
}

const tables_library = [
    { table_name: "rfm_data",
      create_query: query_create_rfm_data_table,
      step: "STEP #2.1:",
      step_info: "rfm_data",
    },
]

const tables_rfm_library = [
    { table_name: "rfm_data_rollup",
      create_query: query_create_rfm_data_rollup_table,
      step: "STEP #1.1:",
      step_info: "rfm_data_rollup",
    },
    { table_name: "rfm_score_recency_data",
      rfm_value: "total_order_value", 
      create_query: query_create_recency_score_table,
      rfm_value: "days_since_last_order", 
      rfm_label: "recency",
      rfm_greater_or_less: ">",
      rfm_range_start_score_low: 205,
      rfm_range_end_score_low: 208,
      rfm_range_start_score_middle: 209,
      rfm_range_end_score_middle: 213,
      step: "STEP #1.2:",
      step_info: "rfm_score_recency_table",
    },
    { table_name: "rfm_score_frequency_data",
      rfm_value: "total_order_value", 
      create_query: query_create_frequency_monetary_score_table,
      rfm_value: "total_number_of_orders",
      rfm_label: "frequency",
      rfm_greater_or_less: "<",
      rfm_range_start_score_low: 0,
      rfm_range_end_score_low: 10,
      rfm_range_start_score_middle: 11,
      rfm_range_end_score_middle: 20,
      step: "STEP #1.3:",
      step_info: "rfm_score_frequency_table",
    },
    { table_name: "rfm_score_monetary_data",
      create_query: query_create_frequency_monetary_score_table,
      rfm_value: "total_order_value",
      rfm_label: "monetary",
      rfm_greater_or_less: "<",
      rfm_range_start_score_low: 0,
      rfm_range_end_score_low: 5000,
      rfm_range_start_score_middle: 5001,
      rfm_range_end_score_middle: 7500,
      step: "STEP #1.4:",
      step_info: "rfm_score_monetary_data",
    },
    { table_name: "rfm_score_summary_data",
      create_query: query_create_rfm_score_summary_table,
      rfm_value: "",
      rfm_label: "",
      rfm_greater_or_less: "",
      step: "STEP #1.5:",
      step_info: "rfm_monetary_score_summary_table",
    },
]

module.exports = {
    query_create_rfm_data_table,
    query_create_rfm_data_rollup_table,
    query_create_recency_score_table,
    query_create_frequency_monetary_score_table,
    query_create_rfm_score_summary_table,
    tables_library,
    tables_rfm_library
}