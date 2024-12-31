#!/bin/bash

# PATH TO JS FILE
# LINUX
JS_FILE="/home/steve-calla/development/ezhire/mysql_load/scheduled_jobs/cron_update_forecast_data/script.js"

# WINDOWS
# JS_FILE="C:/Users/calla/development/ezhire/programs/scheduled_jobs/cron_update_forecast_data/script.js"

# EXECUTE THE JS FILE USING NODE; USE WHICH NODE TO FIND THE PATH
# /usr/bin/node "$JS_FILE"
/home/steve-calla/.nvm/versions/node/v18.20.4/bin/node "$JS_FILE"

# WINDOWS
# /c/Program\ Files/nodejs/node "$JS_FILE"

