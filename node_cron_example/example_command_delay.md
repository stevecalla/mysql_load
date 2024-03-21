@echo off
echo Starting batch file execution...

rem Set the log file path
set LOG_FILE=logfile.txt

rem Command 1 - Replace this with your actual command
echo Running Command 1...
timeout /t 5 /nobreak >nul 2>&1
echo Command 1 completed. >> %LOG_FILE%

rem Command 2 - Replace this with your actual command
echo Running Command 2...
timeout /t 10 /nobreak >nul 2>&1
echo Command 2 completed. >> %LOG_FILE%

rem Command 3 - Replace this with your actual command
echo Running Command 3...
timeout /t 15 /nobreak >nul 2>&1
echo Command 3 completed. >> %LOG_FILE%

echo Batch file execution completed. >> %LOG_FILE%

In this example:

- Replace Command 1, Command 2, Command 3 with your actual commands that you want to run.
- timeout /t <seconds> /nobreak pauses the batch file execution for the specified number of seconds.
- >nul 2>&1 is used to suppress the output of the timeout command.

Adjust the timeout durations (/t <seconds>) as needed based on how long you want to wait between commands.
set LOG_FILE=logfile.txt sets the path and name of the log file. You can change logfile.txt to your desired log file name.

Yes, a .bat file can output information to a log file. You can use the >> redirection operator to append output to a log file. Here's how you can adjust the previous code to log information after each command:

- >> %LOG_FILE% appends the echoed messages to the log file specified by %LOG_FILE%.
- Save the updated code into a .bat file and run it. After each command, the output messages will be appended to the log file specified. Adjust the log file path and format as needed for your requirements.

Save the above code into a file with a .bat extension (e.g., myscript.bat), and then you can schedule this batch file using the Task Scheduler in Windows to run at specific times or intervals. Each command will execute sequentially, waiting for the specified duration between them.

******** GET TIMESTAMP FOR FILE NAME

@echo off
REM Get current date and time in a format suitable for file names
for /f "tokens=2 delims==" %%I in ('wmic os get localdatetime /value') do set "datetime=%%I"
set "timestamp=%datetime:~0,4%%datetime:~4,2%%datetime:~6,2%_%datetime:~8,2%%datetime:~10,2%%datetime:~12,2%"

REM Set the log file path with the timestamp
set LOG_FILE=C:\ProgramData\MySQL\MySQL Server 8.0\Uploads\data\booking_scheduled_report_%timestamp%.txt

echo %LOG_FILE%

In this batch file:

We use wmic os get localdatetime to get the current date and time in a format like YYYYMMDDHHmmss.SSSSSS±UUU.
We extract the relevant parts of the date and time to create a timestamp in the format YYYYMMDD_HHMMSS.

Finally, we use the timestamp to create the log file name by appending it to the file path. The %timestamp% variable in %LOG_FILE% will be replaced with the actual timestamp.
Note: Depending on your date and time format settings in Windows, you may need to adjust the parsing of %datetime% to match the format returned by wmic os get localdatetime.