'use strict';

const AWS = require('aws-sdk'),
      dynamo = new AWS.DynamoDB.DocumentClient();

/*
Expects: {time: UNIX_TIME}
*/

exports.handler = (event, context, callback) => {

    context.callbackWaitsForEmptyEventLoop = false;

    let responseBody = {
        success: false,
    };

    let response = {
        statusCode: 403,
        headers: {
            "Content-Type": 'application/json'
        }
    };

    if (event.queryStringParameters && event.headers['x-api-key'] === 'SECRET_API_KEY') {

        if (event.queryStringParameters.time){
            
            const time = parseInt(event.queryStringParameters.time);

            grabFiles(time) //Retrieve files created before this UNIX time...
            .then(res => {
                responseBody.success = true;
                responseBody.data = res;
                response.statusCode = 200;
                response.body = JSON.stringify(responseBody);
                callback(null, response); 
            })
            .catch(error => {
                responseBody.message = 'Error 1: Invalid Request.' + error;
                response.statusCode = 400;
                response.body = JSON.stringify(responseBody);
                callback(null, response);  
            });

        } else {

            responseBody.message = 'Error 2: Invalid Request. Missing time.';
            response.statusCode = 400;
            response.body = JSON.stringify(responseBody);
            callback(null, response);  

        }
    } else {

        responseBody.message = 'Error 3: Invalid Request. Invalid API key.';
        response.statusCode = 400;
        response.body = JSON.stringify(responseBody);
        callback(null, response);   

    }
};

/*
Grabs items from DynamoDB before a specified timestamp ...
*/
function grabFiles(time) 
{
    return new Promise((resolve, reject) => {
        
        const params = {
            TableName: 'medium_post_demo',
            IndexName: 'item_type-index',
            KeyConditionExpression: '#item_type = :item_type AND  #timeinserted < :from',
            ExpressionAttributeNames: {
                '#item_type': 'item_type',
                '#timeinserted': 'timeinserted'
            },
            ExpressionAttributeValues: {
                ':item_type': 'submission',
                ':from': time
            },
            Limit: 30,
            ScanIndexForward: false
        };

        dynamo.query(params, (err, data) => {
            if (err) {
                reject(err);
            } else {
                console.log(data);
                resolve(data);
            }
        });
    });
}
