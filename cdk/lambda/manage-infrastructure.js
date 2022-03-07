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

const AWS = require('aws-sdk');
const batch = new AWS.Batch();
const cloudwatch = new AWS.CloudWatch();

exports.queryMskClusterThroughput = async function(event) {
  console.log(JSON.stringify(event));
  
  const jobs = {
    jobs: [
      event.depletion_job.JobId
    ]
  };

  const jobDetails = await batch.describeJobs(jobs).promise();

  console.log(JSON.stringify(jobDetails));

  const jobCreatedAt = jobDetails.jobs[0].createdAt;
  const statusSummary = jobDetails.jobs[0].arrayProperties.statusSummary;

  // query max cluster throughput in the last 5 min, but not before the job started
  const queryEndTime = Date.now()
  const queryStartTime = Math.max(queryEndTime - 300_000, jobCreatedAt);

  const params = {
    EndTime: new Date(queryEndTime),
    StartTime: new Date(queryStartTime),
    MetricDataQueries: [
      {
        Id: 'bytesInSum',
        Expression: `SUM(SEARCH('{AWS/Kafka,"Broker ID","Cluster Name"} "Cluster Name"="${process.env.MSK_CLUSTER_NAME}" MetricName="BytesInPerSec"', 'Maximum', 300))`,
        Period: '300',
      },
      {
        Id: 'bytesInStddev',
        Expression: `STDDEV(SEARCH('{AWS/Kafka,"Broker ID","Cluster Name"} "Cluster Name"="${process.env.MSK_CLUSTER_NAME}" MetricName="BytesInPerSec"', 'Minimum', 60))`,
        Period: '60',
      }
    ],
  };

  const metrics = await cloudwatch.getMetricData(params).promise();

  console.log(JSON.stringify(metrics));

  const result = {
    clusterMbInPerSec: metrics.MetricDataResults.find(m => m.Id == 'bytesInSum').Values[0] / 1024**2,
    brokerMbInPerSecStddev: metrics.MetricDataResults.find(m => m.Id == 'bytesInStddev').Values.reduce((acc,v) => v>acc ? v : acc, 0) / 1024**2,
    succeededPlusFailedJobs: statusSummary.SUCCEEDED + statusSummary.FAILED
  };

  console.log(JSON.stringify(result));

  return result;
}

exports.terminateDepletionJob = async function(event) {
  console.log(JSON.stringify(event));
  
  //fixme: change to query pending jobs of the job queue and terminate them

  const params = {
    jobId: event.JobId,
    reason: "Batch job terminated by StepFunctions workflow."
  };

  return batch.terminateJob(params).promise();
}


exports.updateMinCpus = async function(event) {
  console.log(JSON.stringify(event));

  const params = {
    computeEnvironment: process.env.COMPUTE_ENVIRONMENT,
    computeResources: {
      minvCpus: 'current_test' in event ? event.current_test.parameters.num_jobs * 2 : 0
    }
  };

  return batch.updateComputeEnvironment(params).promise();
}