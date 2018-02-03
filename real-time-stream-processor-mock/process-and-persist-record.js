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

	event.Records.forEach(record => {

		let payload = new Buffer(record.kinesis.data, 'base64').toString('ascii');
		
		console.log('* payload', payload);
		let x = JSON.parse(payload);
		console.log('* payload -> JSON', x);
		console.log('* payload -> JSON -> type', typeof(x));
		let y = JSON.parse(x.body);
		console.log('* payload.body -> JSON', y);
		console.log('* payload.body -> JSON -> type', typeof(y));
		
		let activity = y;
		console.log('* activity', activity);

		// Partition key and sort key should be non null values
		if (activity.ip !== null && activity.timestamp !== null) {

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
					console.log('* response -> error:', err);
					callback(err, 'Error in persisting record');
				} else {
					console.log('* response -> data:', data);
					callback(null, data);
				}
			});
		} else {
			callback(null, 'Missing required keys ip and/or timestamp');
		}

	});

}