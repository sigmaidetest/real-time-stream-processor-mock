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
    console.log('***** publish-activity-stream *****');
    console.log('* event:', event, typeof(event));

    // activity reported through API proxy
    let activity = JSON.stringify(event);
    console.log('* activity:', activity, typeof(activity));

    kinesis.putRecord({
        Data: activity,
        PartitionKey: '0',
        StreamName: 'click-stream'
    }).promise()
        .then(data => {
            console.log('* response -> data:', data);
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
        })
        .catch(err => {
            console.log('* response -> error:', err);
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
        });

}