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


import { Construct } from 'constructs';
import { Aws, StackProps, Duration } from 'aws-cdk-lib';
import { aws_iam as iam } from 'aws-cdk-lib';
import { aws_stepfunctions as sfn } from 'aws-cdk-lib';
import { aws_stepfunctions_tasks as tasks } from 'aws-cdk-lib';
import { aws_lambda as lambda } from 'aws-cdk-lib';
import * as batch from '@aws-cdk/aws-batch-alpha';

import { IntegrationPattern } from 'aws-cdk-lib/aws-stepfunctions';

export interface CreditDepletionParameters extends StackProps {
    clusterName: string,
    jobDefinition: batch.IJobDefinition,
    jobQueue: batch.IJobQueue,
    commandParameters: string[],
    payload: sfn.TaskInput
}
  

export class CreditDepletion extends Construct {

    stateMachine: sfn.StateMachine;

    constructor(scope: Construct, id: string, props: CreditDepletionParameters) {
        super(scope, id);


        const fail = new sfn.Fail(this, 'FailDepletion');
        const succeed = new sfn.Succeed(this, 'SucceedDepletion');

        // the number of jobs is determined before all jobs are manually failed, so if there were already failed or succeeded jobs, the depletion exited early and needs to be retried
        const checkAllJobsRunning = new sfn.Choice(this, 'CheckAllJobsRunning')
            .when(sfn.Condition.numberGreaterThan('$.cluster_throughput.Payload.succeededPlusFailedJobs', 0), fail)
            .otherwise(succeed);


        const terminateCreditDepletionLambda = new lambda.Function(this, 'TerminateCreditDepletionLambda', { 
            runtime: lambda.Runtime.NODEJS_14_X,
            code: lambda.Code.fromAsset('lambda'),
            timeout: Duration.seconds(5),
            handler: 'manage-infrastructure.terminateDepletionJob',
        });

        terminateCreditDepletionLambda.addToRolePolicy(
            new iam.PolicyStatement({ 
                actions: ['batch:TerminateJob'],
                resources: ['*']
            })
        );
      
        const terminateCreditDepletion = new tasks.LambdaInvoke(this, 'TerminateCreditDepletion', {
            lambdaFunction: terminateCreditDepletionLambda,
            inputPath: '$.depletion_job',
            resultPath: 'DISCARD'
        });

        terminateCreditDepletion.next(checkAllJobsRunning);

      
        const queryClusterThroughputLambda = new lambda.Function(this, 'QueryClusterThroughputLambda', { 
            runtime: lambda.Runtime.NODEJS_14_X,
            code: lambda.Code.fromAsset('lambda'),
            timeout: Duration.seconds(5),
            handler: 'manage-infrastructure.queryMskClusterThroughput',
            environment: {
              MSK_CLUSTER_NAME: props.clusterName
            }
          });
      
        queryClusterThroughputLambda.addToRolePolicy(
            new iam.PolicyStatement({ 
              actions: ['cloudwatch:GetMetricData', 'batch:DescribeJobs'],
              resources: ['*']
            })
          );
      
        const queryClusterThroughputLowerThan = new tasks.LambdaInvoke(this, 'QueryClusterThroughputLowerThan', {
            lambdaFunction: queryClusterThroughputLambda,
            inputPath: '$',
            resultPath: '$.cluster_throughput'
          });
      
        const waitThroughputLowerThan = new sfn.Wait(this, 'WaitThroughputLowerThan', {
            time: sfn.WaitTime.duration(Duration.minutes(1))
        });
      
        waitThroughputLowerThan.next(queryClusterThroughputLowerThan);
      
      
        const checkThroughputLowerThan = new sfn.Choice(this, 'CheckThroughputLowerThanThreshold')
            .when(sfn.Condition.numberGreaterThan('$.cluster_throughput.Payload.succeededPlusFailedJobs', 0), terminateCreditDepletion)
            .when(
                sfn.Condition.and(
                    sfn.Condition.numberLessThanJsonPath('$.cluster_throughput.Payload.clusterMbInPerSec', '$.test_specification.depletion_configuration.lower_threshold.mb_per_sec'),
//                    sfn.Condition.numberLessThanJsonPath('$.cluster_throughput.Payload.brokerMbInPerSecStddev', '$.test_specification.depletion_configuration.lower_threshold.max_broker_stddev')
                ), 
                terminateCreditDepletion
            )
            .otherwise(waitThroughputLowerThan);
      
        queryClusterThroughputLowerThan.next(checkThroughputLowerThan);
      

        const queryClusterThroughputExceeded = new tasks.LambdaInvoke(this, 'QueryClusterThroughputExceeded', {
            lambdaFunction: queryClusterThroughputLambda,
            inputPath: '$',
            resultPath: '$.cluster_throughput'
        });
      
        const waitThroughputExceeded = new sfn.Wait(this, 'WaitThroughputExceeded', {
            time: sfn.WaitTime.duration(Duration.minutes(1))
        });
      
        waitThroughputExceeded.next(queryClusterThroughputExceeded);


        const checkLowerThanPresent = new sfn.Choice(this, 'CheckLowerThanPresent')
            .when(sfn.Condition.isPresent('$.test_specification.depletion_configuration.lower_threshold.mb_per_sec'), waitThroughputLowerThan)
            .otherwise(terminateCreditDepletion);
      
      
        const checkThroughputExceeded = new sfn.Choice(this, 'CheckThroughputExceeded')
            .when(sfn.Condition.numberGreaterThan('$.cluster_throughput.Payload.succeededPlusFailedJobs', 0), terminateCreditDepletion)
            .when(sfn.Condition.numberGreaterThanJsonPath('$.cluster_throughput.Payload.clusterMbInPerSec', '$.test_specification.depletion_configuration.upper_threshold.mb_per_sec'), checkLowerThanPresent)
            .otherwise(waitThroughputExceeded);
      
        queryClusterThroughputExceeded.next(checkThroughputExceeded);
      

      
        const submitCreditDepletion = new tasks.BatchSubmitJob(this, 'SubmitCreditDepletion', {
            jobName: 'RunCreditDepletion',
            jobDefinitionArn: props.jobDefinition.jobDefinitionArn,
            jobQueueArn: props.jobQueue.jobQueueArn,
            arraySize: sfn.JsonPath.numberAt('$.current_test.parameters.num_jobs'),
            containerOverrides: {
              command: [...props.commandParameters, '--command', 'deplete-credits' ]
            },
            payload: props.payload,
            integrationPattern: IntegrationPattern.REQUEST_RESPONSE,
            inputPath: '$',
            resultPath: '$.depletion_job',
        });
      
        submitCreditDepletion.next(waitThroughputExceeded);


        const updateDepletionParameterLambda = new lambda.Function(this, 'UpdateDepletionParameterLambda', { 
            runtime: lambda.Runtime.PYTHON_3_8,
            code: lambda.Code.fromAsset('lambda'),
            timeout: Duration.seconds(5),
            handler: 'test-parameters.update_parameters_for_depletion',
        });
    
        const updateDepletionParameter = new tasks.LambdaInvoke(this, 'UpdateDepletionParameter', {
            lambdaFunction: updateDepletionParameterLambda,
            outputPath: '$.Payload',
        });

        updateDepletionParameter.next(submitCreditDepletion);
  
        const checkDepletion = new sfn.Choice(this, 'CheckDepletionRequired')
            .when(sfn.Condition.isNotPresent('$.test_specification.depletion_configuration'), succeed)
            .otherwise(updateDepletionParameter);
      
        this.stateMachine = new sfn.StateMachine(this, 'DepleteCreditsStateMachine', {
            definition: checkDepletion,
            stateMachineName: `${Aws.STACK_NAME}-credit-depletion`,
        }); 
    }
}
