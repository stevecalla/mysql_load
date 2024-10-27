#!/bin/bash

# Load environment variables from .env file
# export $(cat /home/steve/development/ezhire/mysql_load/.env | xargs)

# PATH TO JS FILE
JS_FILE="/home/steve/development/ezhire/mysql_load/scheduled_jobs/cron_daily_bookings/script.js"

# SLACK_BOOKINGS_POST=" "

# EXECUTE THE JS FILE USING NODE
/usr/bin/node "$JS_FILE"

# /usr/bin/node "$SLACK_BOOKINGS_POST"