# *
# * This Source Code Form is subject to the terms of the Mozilla Public
# * License, v. 2.0. If a copy of the MPL was not distributed with this
# * file, You can obtain one at http://mozilla.org/MPL/2.0/.
# *

# Dockerfile for OED database server.
# Uses TimescaleDB (PostgreSQL 15 + time-series extension) for optimized
# handling of meter readings with automatic partitioning and compression.
FROM timescale/timescaledb:2.13.0-pg15

# All SQL files in the build context
# get copied into the container and
# run on init.
COPY *.sql /docker-entrypoint-initdb.d/
