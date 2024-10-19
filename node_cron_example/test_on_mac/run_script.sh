#!/bin/bash

# PATH TO JS FILE
JS_FILE="/Users/teamkwsc/development/ezhire/mysql_load/node_cron_example/test_on_mac/script.js"

TEST_SLACK_BOOKINGS_POST="/Users/teamkwsc/development/ezhire/mysql_load/scheduled_jobs/daily_bookings_by_hour.js"

# EXECUTE THE JS FILE USING NODE
node "$JS_FILE"

node "$TEST_SLACK_BOOKINGS_POST"