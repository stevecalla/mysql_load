REM "c:/Users/calla/Google Drive/Resume & Stuff/ezhire/sql_analysis/programs/scheduled_jobs/daily_bookings_by_hour.bat"
REM @echo off (is commented out so the commands below will print in the console)
@echo

REM Set the working directory as below to the correct path. This will ensure MySQL can connect.
cd "C:/Users/calla/Google Drive/Resume & Stuff/ezhire/sql_analysis/programs/scheduled_jobs"

REM Run the javascript file
node "C:\Users\calla\Google Drive\Resume & Stuff\ezhire\sql_analysis\programs\scheduled_jobs\daily_bookings_by_hour.js"

REM Set to pause to review the logs and process that executed. Remove pause if this is not necessary.
pause

REM Note: For this bat file to work, the working directory needs to be set to the location of the files. This will allow it to access MySQL.See this line 