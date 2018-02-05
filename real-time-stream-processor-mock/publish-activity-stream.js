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
const kinesis = new AWS.Kinesis();

exports.handler = function (event, context, callback) {
	console.debug('Event:', event);

    // activity reported through API proxy
    let activity = JSON.stringify(event.body);
    console.log('Activity:', activity);

    kinesis.putRecord({
        Data: activity,
        PartitionKey: '0',
        StreamName: 'click-stream'
    }).promise()
        .then(data => {
            console.debug('Response -> data:', data);
            let response = {
                'statusCode': 200,
                'headers': {
                    'Access-Control-Allow-Origin': '*',
                    'Content-Type': 'application/json'
                },
                'body': JSON.stringify({
                    'Code': 'PutRecordSuccessful',
                    'Message': 'Record was successfully put to stream click-stream',
                    'Data': data
                }),
                'isBase64Encoded': false
            };
            callback(null, response);
        })
        .catch(err => {
            console.debug('Response -> error:', err);
            let response = {
                'statusCode': err.statusCode,
                'headers': {
                    'Access-Control-Allow-Origin': '*',
                    'Content-Type': 'application/json'
                },
                'body': JSON.stringify({
                    'Code': err.code,
                    'Message': err.message
                }),
                'isBase64Encoded': false
            };
            callback(null, response);
        });

}