function query_drop_rfm_score_summary_history_data_tracking_backup(table) {
	return `DROP TABLE IF EXISTS ${table}_backup;`
};

function query_create_rfm_score_summary_history_data_tracking_backup() {
	return `CREATE TABLE ${table}_backup LIKE ${table};`
};

function query_insert_rfm_score_summary_history_data_tracking_backup() {
	return `INSERT INTO ${table}_backup SELECT * FROM ${table};`
};

module.exports = { 
	query_drop_rfm_score_summary_history_data_tracking_backup,
	query_create_rfm_score_summary_history_data_tracking_backup,
	query_insert_rfm_score_summary_history_data_tracking_backup,
};