REM @echo off (is commented out so the commands below will print in the console)
@echo

REM go to root director
cd ../..

REM list out file in director
dir 

cd C:\Program Files\MySQL\MySQL Server 8.0\bin
REM mysql -u user -p

REM log into MySQL
mysql -u root -pdenverdenver

REM log into MySQL & execute select query
mysql -u root -pdenverdenver -e "SELECT * FROM ezhire_pacing_metrics.calendar_table LIMIT 1;"

REM USE ezhire_pacing_metrics;
REM SELECT * FROM calendar_table LIMIT 1; 

REM EXIT (added EXIT below to close the command line interface)
pause

REM echo Exit from MYSQL...
REM echo Exit from MYSQL...