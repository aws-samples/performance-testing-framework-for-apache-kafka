// Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// 
// Permission is hereby granted, free of charge, to any person obtaining a copy of
// this software and associated documentation files (the "Software"), to deal in
// the Software without restriction, including without limitation the rights to
// use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
// the Software, and to permit persons to whom the Software is furnished to do so.
// 
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
// FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
// COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
// IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
// CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

const AWS = require('aws-sdk')
const batch = new AWS.Batch();
const cloudwatchlogs = new AWS.CloudWatchLogs();

exports.queryProducerOutput = async function(event) {
    console.log(JSON.stringify(event));

    const numProducer = event.current_test.parameters.num_producers
    const jobId = event.job_result.JobId

    const describeParams = {
        jobs: [...Array(numProducer).keys()].map(x => jobId + ":" + x)
    }    

    const jobDecriptions = await batch.describeJobs(describeParams).promise();
    
    const logStreams = jobDecriptions.jobs.map(job => job.attempts[0].container.logStreamName);

    console.log(JSON.stringify(logStreams));

    const filterParams = {
        logGroupName: process.env.LOG_GROUP_NAME,
        logStreamNames: logStreams,
        filterPattern: '{$.type="producer" && $.test_summary=* && $.error_count=*}',
    }

    return queryAndParseLogs(filterParams, numProducer);
}


async function queryAndParseLogs(filterParams, numProducer) {
    let filteredEvents = await cloudwatchlogs.filterLogEvents(filterParams).promise();

    console.log(JSON.stringify(filteredEvents));

    let queryResult = filteredEvents.events;

    // obtain all results through pagination
    while ('nextToken' in filteredEvents) {
        filteredEvents = await cloudwatchlogs.filterLogEvents({...filterParams, nextToken: filteredEvents.nextToken}).promise();

        console.log(JSON.stringify(filteredEvents));

        queryResult = [...queryResult, ...filteredEvents.events]
    }

    console.log(JSON.stringify(queryResult));

    if (queryResult.length < numProducer) {
        // if we didn't obtain a result for all producer tasks, fail so that the stepfunctions workflow can retry later
        throw new Error(`incomplete query results: expected ${numProducer} results, found ${queryResult.length}`)
    } else {
        const testResults = queryResult.map(e => JSON.parse(e.message));

        console.log(JSON.stringify(testResults));

        // aggregate number of timeout errors
        const errorCountSum = testResults.reduce((acc,message) => acc + parseInt(message.error_count), 0)
        // parse producer throughput
        const mbPerSec = testResults
            .map(message => message.test_summary.match(/([0-9.]+) MB\/sec/))
            .map(match => parseFloat(match[1]))

        return {
            errorCountSum: errorCountSum,
            mbPerSecSum: mbPerSec.reduce((acc,v) => acc + v, 0),
            mbPerSecMin: mbPerSec.reduce((acc,v) => v<acc ? v : acc, Number.MAX_VALUE),
        }
    }
}