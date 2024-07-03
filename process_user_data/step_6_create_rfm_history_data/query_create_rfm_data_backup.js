function query_create_rfm_table_backup() {
	return `CREATE TABLE rfm_score_summary_history_data_backup LIKE rfm_score_summary_history_data;`
};

function query_insert_rfm_history_data_backup() {
	return `INSERT INTO rfm_score_summary_history_data_backup SELECT * FROM rfm_score_summary_history_data;`
};

function query_drop_rfm_table_backup() {
	return `DROP TABLE IF EXISTS rfm_score_summary_history_data_backup;`
};

module.exports = { 
	query_create_rfm_table_backup,
	query_insert_rfm_history_data_backup,
	query_drop_rfm_table_backup,
};