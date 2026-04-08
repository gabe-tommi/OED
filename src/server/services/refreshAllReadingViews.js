/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

const { log } = require('../log');
const { getConnection } = require('../db');
const Reading = require('../models/Reading');

/**
 * Refreshes all reading aggregate views.
 * This supports both normal materialized views and Timescale continuous aggregates.
 */
async function refreshAllReadingViews() {
	const conn = getConnection();
	// Refresh meter readings views
	log.info('Refreshing meter aggregate reading views');
	await Reading.refreshMeterReadingsViews(conn);
	log.info('Meter aggregate reading views refreshed');
	// Refresh group views
	log.info('Refreshing group aggregate reading views');
	await Reading.refreshGroupReadingsViews(conn);
	log.info('refreshAllReadingViews completed');
}

module.exports = { refreshAllReadingViews };
