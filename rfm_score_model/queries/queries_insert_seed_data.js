function query_insert_seed_data(table_name, columns, values) {
    return(`INSERT INTO ${table_name} (${columns}) VALUES (${values});`);
}

module.exports = {
    query_insert_seed_data
}