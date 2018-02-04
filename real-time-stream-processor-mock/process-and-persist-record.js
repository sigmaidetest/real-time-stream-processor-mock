let AWS = require('aws-sdk');
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

const ddb = new AWS.DynamoDB.DocumentClient();

exports.handler = function (event, context, callback) {
	console.log('Event:', event);

	event.Records.forEach(record => {
		console.log('Data:', record.Kinesis.data);
		let request = JSON.parse(new Buffer(record.kinesis.data, 'base64').toString('ascii'));
		let activity = JSON.parse(request);
		console.log('Activity:', activity);

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
					console.log('Response -> error:', err);
					let response = {
						'statusCode': err.statusCode,
						'headers': {
							'Access-Control-Allow-Origin': '*',
							'Content-Type': 'application/json'
						},
						'body': {
							'Code': err.code,
							'Message': err.message
						},
						'isBase64Encoded': false
					};
					callback(null, response);
				} else {
					console.log('Response -> data:', data);
					let response = {
						'statusCode': 200,
						'headers': {
							'Access-Control-Allow-Origin': '*',
							'Content-Type': 'application/json'
						},
						'body': {
							'Code': 'PutRecordSuccessful',
							'Message': 'Record was successfully put to stream click-stream',
							'data': JSON.stringify(data)
						},
						'isBase64Encoded': false
					};
					callback(null, response);
				}
			});

		} else {
			callback(null, 'Missing required keys ip and/or timestamp');
		}

	});

}