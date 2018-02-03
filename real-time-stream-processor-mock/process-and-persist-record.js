/*
 * SLAppForge Sigma. Integrated Development Environment for Serverless Computing
 *
 * Copyright (c) 2018 SLAppForge Lanka Private Limited. All Rights Reserved.
 * https://www.slappforge.com
 *
 * You may use this file in accordance with the License Agreement provided with the Software or, alternatively, 
 * in accordance with the terms contained in a written agreement between you and SLAppForge.
 *
 * If you have questions regarding the use of this file, please contact SLAppForge at info@slappforge.com
 */

const AWS = require('aws-sdk');
const ddb = new AWS.DynamoDB.DocumentClient();

exports.handler = function (event, context, callback) {
    console.log('\n***event:\n', event, '\n***\n');

	event.Records.forEach(record => {
		console.log('\n***record:\n', record, '\n***\n');

		let payload = new Buffer(record.kinesis.data, 'base64').toString('ascii');
		console.log('\n***payload:\n', payload, '\n***\n');
		let activity = JSON.parse(payload);
		console.log('\n***activity:\n', activity, '\n***\n');

		// Partition key and sort key should be non null values
		if (activity.ip !== null && activity.datetime !== null) {

			ddb.put({
				TableName: 'click-stream-table',
				Item: {
					'IP': activity.ip,
					'Timestamp': activity.timestamp,
					'Browser': activity.browser,
					'URL': activity.url,
					'Referrer': activity.referrer,
					'OS': activity.os,
					'Country': activity.country,
					'Language': activity.language
				}
			}, function (err, data) {
				if (err) {
					console.log('\n***err:\n', err, '\n***\n');
					callback(err, 'Error in persisting record');
				} else {
					console.log('\n***data:\n', data, '\n***\n');
					callback(null, data);
				}
			});
		} else {
			callback(null, 'Missing required keys ip and/or timestamp');
		}

	});

}