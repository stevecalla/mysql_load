@echo
REM Get current date and time in a format suitable for file names
for /f "tokens=2 delims==" %%I in ('wmic os get localdatetime /value') do set "datetime=%%I"

set "timestamp=%datetime:~0,4%%datetime:~4,2%%datetime:~6,2%_%datetime:~8,2%%datetime:~10,2%%datetime:~12,2%"

REM Set the log file path i.e. set LOG_FILE=logfile.txt
set LOG_FILE=C:\ProgramData\MySQL\MySQL Server 8.0\Uploads\data\logs\booking_scheduled_report_%timestamp%.txt`

REM Check if the log directory exists, create it if not
IF NOT EXIST "C:\ProgramData\MySQL\MySQL Server 8.0\Uploads\data\logs" (
    mkdir "C:\ProgramData\MySQL\MySQL Server 8.0\Uploads\data\logs"
)

REM STEP #1: GET BOOKING DATA
node "C:\Users\calla\Google Drive\Resume & Stuff\ezhire\sql_analysis\programs\get_booking_data\sql_getBookingData_ssh_loop.js"
REM 60 secs * 4 min = 240
timeout /t 240 /nobreak >nul 2>&1
echo Command #1 retrieving booking data is complete.  >> %LOG_FILE%

REM STEP #2: LOAD BOOKING DATA
node "C:\Users\calla\Google Drive\Resume & Stuff\ezhire\sql_analysis\programs\load_booking_data\sql_load_bookingData.js"
REM 60 secs * 1 min = 60
timeout /t 60 /nobreak >nul 2>&1
echo Command #2 loading booking data is complete.  >> %LOG_FILE%

REM STEP #3: CREATE KEY METRICS / ON RENT DATA
node "C:\Users\calla\Google Drive\Resume & Stuff\ezhire\sql_analysis\programs\create_keyMetrics_data\sql_getKeyMetrics_loop.js"
REM 60 secs * 1 min = 60; CAN EXECUTE AS SOON AS STEP #2 IS DONE
timeout /t 60 /nobreak >nul 2>&1
echo Command #3 creating key metrics data is complete.  >> %LOG_FILE%

REM STEP #4: CREATE PACING DATA
node "C:\Users\calla\Google Drive\Resume & Stuff\ezhire\sql_analysis\programs\create_pacing_data\sql_getPacingMetrics_loop.js"
REM 60 secs * 1 min = 60; CAN EXECUTE AS SOON AS STEP #2 IS DONE
timeout /t 60 /nobreak >nul 2>&1
echo Command #4 creating pacing data is complete. >> %LOG_FILE%

echo Batch file execution completed. >> %LOG_FILE%

pause

@echo
REM Get current date and time in a format suitable for file names
for /f "tokens=2 delims==" %%I in ('wmic os get localdatetime /value') do set "datetime=%%I"
set "timestamp=%datetime:~0,4%%datetime:~4,2%%datetime:~6,2%_%datetime:~8,2%%datetime:~10,2%%datetime:~12,2%"

REM Set the log file path with the timestamp
set LOG_FILE=C:\ProgramData\MySQL\MySQL Server 8.0\Uploads\data\log\booking_scheduled_report_%timestamp%.txt

REM Check if the log directory exists, create it if not
IF NOT EXIST "C:\ProgramData\MySQL\MySQL Server 8.0\Uploads\data\logs" (
    mkdir "C:\ProgramData\MySQL\MySQL Server 8.0\Uploads\data\logs"
)

echo Starting batch file execution... > %LOG_FILE%

REM STEP #1: GET BOOKING DATA
echo Getting booking data...
setlocal enabledelayedexpansion
set "start_time=!time!"
node "C:\Users\calla\Google Drive\Resume & Stuff\ezhire\sql_analysis\programs\get_booking_data\sql_getBookingData_ssh_loop.js"
set "end_time=!time!"

REM Calculate duration
call :calculate_duration "!start_time!" "!end_time!" "Step #1: Getting booking data"

echo Command #1 retrieving booking data is complete. >> %LOG_FILE%

REM STEP #2: LOAD BOOKING DATA
echo Loading booking data...
set "start_time=!time!"
node "C:\Users\calla\Google Drive\Resume & Stuff\ezhire\sql_analysis\programs\load_booking_data\sql_load_bookingData.js"
set "end_time=!time!"

REM Calculate duration
call :calculate_duration "!start_time!" "!end_time!" "Step #2: Loading booking data"

echo Command #2 loading booking data is complete. >> %LOG_FILE%

REM STEP #3: CREATE KEY METRICS / ON RENT DATA
echo Creating key metrics data...
set "start_time=!time!"
node "C:\Users\calla\Google Drive\Resume & Stuff\ezhire\sql_analysis\programs\create_keyMetrics_data\sql_getKeyMetrics_loop.js"
set "end_time=!time!"

REM Calculate duration
call :calculate_duration "!start_time!" "!end_time!" "Step #3: Creating key metrics data"

echo Command #3 creating key metrics data is complete. >> %LOG_FILE%

REM STEP #4: CREATE PACING DATA
echo Creating pacing data...
set "start_time=!time!"
node "C:\Users\calla\Google Drive\Resume & Stuff\ezhire\sql_analysis\programs\create_pacing_data\sql_getPacingMetrics_loop.js"
set "end_time=!time!"

REM Calculate duration
call :calculate_duration "!start_time!" "!end_time!" "Step #4: Creating pacing data"

echo Command #4 creating pacing data is complete. >> %LOG_FILE%

echo Batch file execution completed. >> %LOG_FILE%

pause
exit /b

:calculate_duration
set "start_time=%~1"
set "end_time=%~2"
set "step_name=%~3"

REM Extract hours, minutes, seconds, and milliseconds from the time strings
for /f "tokens=1-4 delims=:.," %%a in ("%start_time%") do set /a start_time=(((%%a * 60 + 1%%b %% 100) * 60 + 1%%c %% 100) * 100 + 1%%d %% 100) - 1
for /f "tokens=1-4 delims=:.," %%a in ("%end_time%") do set /a end_time=(((%%a * 60 + 1%%b %% 100) * 60 + 1%%c %% 100) * 100 + 1%%d %% 100) - 1

REM Calculate the duration
set /a duration=%end_time% - %start_time%
set /a hours=%duration% / 360000
set /a minutes=(%duration% / 6000) %% 60
set /a seconds=(%duration% / 100) %% 60
set /a milliseconds=%duration% %% 100

REM Output the duration to the log file
echo %step_name% took %hours% hours, %minutes% minutes, %seconds% seconds, and %milliseconds% milliseconds. >> %LOG_FILE%
exit /b
