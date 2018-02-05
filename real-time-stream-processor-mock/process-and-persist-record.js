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
	console.debug('Event:', event);

	event.Records.forEach(record => {
		
		let request = JSON.parse(new Buffer(record.kinesis.data, 'base64').toString('ascii'));
		let activity = JSON.parse(request);
		console.debug('Activity:', activity);

		// Partition key and sort key should be non null values
		if (activity.ip !== undefined && activity.timestamp !== undefined) {

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
					console.debug('Response -> error:', err);
					callback(null, err.message);
				} else {
					console.debug('Response -> data:', data);
					callback(null, 'Record was successfully put to click-stream');
				}
			});

		} else {
			callback(null, 'Missing required keys ip and/or timestamp');
		}

	});

}