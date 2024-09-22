REM @echo off (is commented out so the commands below will print in the console)
@echo off

REM go to root directory
cd ../..

REM list out file in director
dir 

REM log into MySQL
REM mysql -u root -pdenverdenver

REM log into MySQL & execute select query
cd C:\Program Files\MySQL\MySQL Server 8.0\bin
mysql -u root -pdenverdenver -e "USE ezhire_pacing_metrics; SELECT * FROM calendar_table LIMIT 1;"

REM EXIT (added EXIT below to close the command line interface)
pause

REM echo Exit from MYSQL...
REM echo Exit from MYSQL...