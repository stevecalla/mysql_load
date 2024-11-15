[1mdiff --git a/scheduled_jobs/cron_scheduled_leads/run_script.sh b/scheduled_jobs/cron_scheduled_leads/run_script.sh[m
[1mold mode 100644[m
[1mnew mode 100755[m
[1mdiff --git a/scheduled_jobs/daily_bookings_by_hour.js b/scheduled_jobs/daily_bookings_by_hour.js[m
[1mindex b836439..a973fd8 100755[m
[1m--- a/scheduled_jobs/daily_bookings_by_hour.js[m
[1m+++ b/scheduled_jobs/daily_bookings_by_hour.js[m
[36m@@ -12,7 +12,7 @@[m [mconst { check_most_recent_created_on_date } = require('../get_most_recent_create[m
 const { execute_get_daily_booking_data } = require('../daily_booking_forecast/step_1_sql_get_daily_booking_data'); //step_1[m
 [m
 // TESTING VARIABLES[m
[31m-let send_slack_to_calla = false;[m
[32m+[m[32mlet send_slack_to_calla = true;[m
 let is_testing = false; // allows for testing of is_within_15_minutes in check_most_recent_created_on_date.js[m
 [m
 // RUN PROGRAM[m
[1mdiff --git a/utilities/server.js b/utilities/server.js[m
[1mindex 10a6152..2c78fd9 100644[m
[1m--- a/utilities/server.js[m
[1m+++ b/utilities/server.js[m
[36m@@ -87,7 +87,7 @@[m [mapp.post('/get-leads', async (req, res) => {[m
 // Endpoint to handle slash "/leads" command[m
 app.get('/scheduled-leads', async (req, res) => {[m
     // TESTING VARIABLES[m
[31m-    let send_slack_to_calla = true;[m
[32m+[m[32m    let send_slack_to_calla = false;[m
 [m
     try {[m
         const getResults = await execute_get_daily_lead_data();[m
