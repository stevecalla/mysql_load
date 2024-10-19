#!/bin/bash

# Load environment variables from .env file
export $(cat /Users/teamkwsc/development/ezhire/mysql_load/.env | xargs)

# PATH TO JS FILE
JS_FILE="/Users/teamkwsc/development/ezhire/mysql_load/scheduled_jobs/cron_daily_bookings/script.js"

SLACK_BOOKINGS_POST="/Users/teamkwsc/development/ezhire/mysql_load/scheduled_jobs/daily_bookings_by_hour.js"

# EXECUTE THE JS FILE USING NODE
/Users/teamkwsc/.nvm/versions/node/v17.9.1/bin/node "$JS_FILE"

/Users/teamkwsc/.nvm/versions/node/v17.9.1/bin/node "$SLACK_BOOKINGS_POST"