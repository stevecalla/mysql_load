#!/bin/bash

# Load environment variables from .env file
# export $(cat /home/steve/development/ezhire/mysql_load/.env | xargs)

# PATH TO JS FILE
JS_FILE="/home/steve-calla/development/ezhire/mysql_load/scheduled_jobs/cron_daily_bookings/script.js"

# SLACK_BOOKINGS_POST=""

# EXECUTE THE JS FILE USING NODE; USE WHICH NODE TO FIND THE PATH
# /usr/bin/node "$JS_FILE"
/home/steve-calla/.nvm/versions/node/v18.20.4/bin/node "$JS_FILE"

# /usr/bin/node "$SLACK_BOOKINGS_POST"