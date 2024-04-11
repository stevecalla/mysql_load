REM @echo off (is commented out so the commands below will print in the console)
@echo
REM START C:\Windows\System32\calc.exe

REM Doesn't work. Need to setup stunnel per below
REM https://blat.yahoogroups.narkive.com/0Hz7Zm95/gmail
REM https://www.blat.net/examples/stunnelConfig.html


START wmplayer C:\Users\calla\Downloads\game-start-6104.mp3

set "recipient=callasteven@gmail.com"
set "subject=Test Email"
set "body=This is a test email sent via Blat."
REM set "attachment=C:\path\to\attachment.txt"
set "server=smtp.gmail.com"
set "sender=callasteven@gmail.com"
REM set "debug=-debug -log blat.log -timestamp"

REM (with attachment) blat -to "%recipient%" -subject "%subject%" -body "%body%" -server "%server%" -f "%sender%" -attach "%attachment%" "%debug%"

blat -install smtp.gmail.com your_email@gmail.com -u your_email@gmail.com -pw your_password -port 465 -ssl


blat -to "%recipient%" -subject "%subject%" -body "%body%" -server "%server%" -f "%sender%"

REM blat email.txt -to callasteven@gmail.com -subject "Test Email" -server smtp.gmail.com -f callasteven@gmail.com

pause

REM This is a comment
REM The echo off command in a batch file is used to turn off the echoing of commands. By default, when a batch file is executed, each command in the batch file is displayed on the console before it is executed. This can be useful for debugging or seeing the progress of the batch file, but in some cases, you may not want the commands to be displayed.

REM When you use echo off at the beginning of a batch file, it prevents the commands themselves from being displayed on the console as they are executed. This can make the output cleaner and more focused on the actual results of the batch file's execution rather than the individual commands being run.

REM To prevent the command prompt window from closing automatically after executing a batch file (.bat), you can add the pause command at the end of the batch file. This will display a message like "Press any key to continue..." and wait for user input before closing the window. Here's an example:
