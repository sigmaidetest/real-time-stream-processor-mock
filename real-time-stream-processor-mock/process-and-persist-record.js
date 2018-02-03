let AWS = require('aws-sdk');
const ddb = new AWS.DynamoDB.DocumentClient();
exports.handler = function (event, context, callback) {

	event.Records.forEach(record => {

		let payload = JSON.parse(new Buffer(record.kinesis.data, 'base64').toString('ascii'));

		if (payload.ip !== null && payload.datetime !== null) {

			ddb.put({
				TableName: 'click-stream-table',
				Item: {
					'IP': payload.ip,
					'Timestamp': payload.timestamp,
					'Browser': payload.browser,
					'URL': payload.url,
					'Referrer': payload.referrer,
					'OS': payload.os,
					'Country': payload.country,
					'Language': payload.language
				}
			}, function (err, data) {
				if (err) {
					callback(err, 'Error in persisting record');
				} else {
					callback(null, data);
				}
			});
		} else {
			callback(null, 'Missing required keys ip or datetime');
		}

	});

}