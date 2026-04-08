# This script forces a refresh of all four reading aggregate materialized views:
#   hourly_readings_unit, daily_readings_unit,
#   group_hourly_readings_unit, group_daily_readings_unit
#
# The readings table is a TimescaleDB hypertable, but the aggregate views are
# standard PostgreSQL materialized views and must be refreshed on a schedule.
# Refresh order matters: hourly must finish before daily, since daily is built on top of it.
#
# Copy this file to /etc/cron.daily/ (or equivalent) to run it automatically each night.

# The absolute path the project root directory (OED)
cd '/example/path/to/project/OED'

# The following line should NOT need to be edited except by devs or if you have an old system with only docker-compose.
docker compose run --rm web npm run --silent refreshReadingViews &>> /dev/null &
