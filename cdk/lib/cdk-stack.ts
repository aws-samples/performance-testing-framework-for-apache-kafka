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
import { Aws, Stack, StackProps, CfnResource,  CfnOutput, Duration, RemovalPolicy } from 'aws-cdk-lib';
import { aws_ecs as ecs } from 'aws-cdk-lib';
import { aws_ec2 as ec2 } from 'aws-cdk-lib';
import { aws_iam as iam } from 'aws-cdk-lib';
import { aws_logs as logs } from 'aws-cdk-lib';
import { aws_stepfunctions as sfn } from 'aws-cdk-lib';
import { aws_stepfunctions_tasks as tasks } from 'aws-cdk-lib';
import { aws_lambda as lambda } from 'aws-cdk-lib';
import { aws_cloudwatch as cloudwatch} from 'aws-cdk-lib';
import { aws_sagemaker as sagemaker } from 'aws-cdk-lib';
import { custom_resources } from 'aws-cdk-lib';

import * as msk from '@aws-cdk/aws-msk-alpha';
import * as batch from '@aws-cdk/aws-batch-alpha';

import { InstanceType } from 'aws-cdk-lib/aws-ec2';
import { RetentionDays } from 'aws-cdk-lib/aws-logs';
import { IntegrationPattern, TaskInput } from 'aws-cdk-lib/aws-stepfunctions';
import { AwsCustomResource } from 'aws-cdk-lib/custom-resources';
import { CreditDepletion } from './credit-depletion-sfn';
import { ClusterMonitoringLevel, KafkaVersion } from '@aws-cdk/aws-msk-alpha';


export interface MskClusterParametersNewCluster extends StackProps {
  clusterProps: {
    numberOfBrokerNodes: number,    //brokers per AZ
    instanceType: ec2.InstanceType,
    ebsStorageInfo: msk.EbsStorageInfo,
    encryptionInTransit: msk.EncryptionInTransitConfig,
    clientAuthetication?: msk.ClientAuthentication,
    kafkaVersion: KafkaVersion,
    configurationInfo?: msk.ClusterConfigurationInfo
  },
  vpc?: ec2.IVpc,
  sg?: ec2.ISecurityGroup,
  initialPerformanceTest?: any,
}

export interface BootstrapBroker extends StackProps {
  bootstrapBrokerString: string,
  clusterName: string,
  vpc?: ec2.IVpc | string,
  sg: ec2.ISecurityGroup | string,
  initialPerformanceTest?: any,
}



function toStackName(params: MskClusterParametersNewCluster | BootstrapBroker, prefix: string) : string {
  if ('bootstrapBrokerString' in params) {
    return prefix
  } else {
    const brokerNodeType = params.clusterProps.instanceType.toString().replace(/[^0-9a-zA-Z]/g, '');
    const inClusterEncryption = params.clusterProps.encryptionInTransit.enableInCluster ? "t" : "f"
    const clusterVersion = params.clusterProps.kafkaVersion.version.replace(/[^0-9a-zA-Z]/g, '');

    const numSubnets = params.vpc ? Math.max(params.vpc.privateSubnets.length, 3) : 3;

    const name = `${params.clusterProps.numberOfBrokerNodes*numSubnets}-${brokerNodeType}-${params.clusterProps.ebsStorageInfo.volumeSize}-${inClusterEncryption}-${clusterVersion}`;

    return `${prefix}${name}`;
  }
}


export class CdkStack extends Stack {
  constructor(scope: Construct, id: string, props: MskClusterParametersNewCluster | BootstrapBroker) {
    super(scope, toStackName(props, id), { ...props, description: 'Creates resources to run automated performance tests against Amazon MSK clusters (shausma-msk-performance)' });

    var vpc;
    if (props.vpc) {
      if (typeof props.vpc === "string") {
        vpc = ec2.Vpc.fromLookup(this, 'Vpc', { vpcId: props.vpc })
      } else {
        vpc = props.vpc
      }
    } else {
      vpc = new ec2.Vpc(this, 'Vpc');
    }

    var sg;
    if (props.sg) {
      if (typeof props.sg === "string") {
        sg = ec2.SecurityGroup.fromSecurityGroupId(this, 'SecurityGroup', props.sg)
      } else {
        sg = props.sg
      }
    } else {
      sg = new ec2.SecurityGroup(this, 'SecurityGroup', { vpc: vpc });
      sg.addIngressRule(sg, ec2.Port.allTraffic());
    }
    
    var kafka : { cfnCluster?: CfnResource, numBrokers?: number, clusterName: string };


    if ('bootstrapBrokerString' in props) {
      kafka = {
        clusterName: props.clusterName
      };
    } else {
      const cluster = new msk.Cluster(this, 'KafkaCluster', {
        ...props.clusterProps,
        monitoring: {
          clusterMonitoringLevel: ClusterMonitoringLevel.PER_BROKER
        },
        vpc: vpc,
        vpcSubnets: {
          subnetType: ec2.SubnetType.PRIVATE_WITH_NAT
        },
        securityGroups: [ sg ],
        clusterName: Aws.STACK_NAME,
        removalPolicy: RemovalPolicy.DESTROY,
      });

      kafka = {
        cfnCluster: cluster.node.findChild('Resource') as CfnResource,
        numBrokers: Math.max(vpc.privateSubnets.length, 3) * props.clusterProps.numberOfBrokerNodes,
        clusterName: cluster.clusterName
      }
    }


    const defaultLogGroup = new logs.LogGroup(this, 'DefaultLogGroup', {
      retention: RetentionDays.ONE_WEEK
    });

    const performanceTestLogGroup = new logs.LogGroup(this, 'PerformanceTestLogGroup', {
      retention: RetentionDays.ONE_YEAR
    });


    const batchEcsInstanceRole = new iam.Role(this, 'BatchEcsInstanceRole', {
      assumedBy: new iam.ServicePrincipal('ec2.amazonaws.com'),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AmazonEC2ContainerServiceforEC2Role'),
        iam.ManagedPolicy.fromAwsManagedPolicyName('AmazonSSMManagedInstanceCore')
      ]
    });

    const batchEcsInstanceProfile = new iam.CfnInstanceProfile(this, 'BatchEcsInstanceProfile', {
      roles: [ batchEcsInstanceRole.roleName ],
    });

    const computeEnvironment = new batch.ComputeEnvironment(this, 'ComputeEnvironment', {
      computeEnvironmentName: `${Aws.STACK_NAME}`,
      computeResources: {
        vpc,
        instanceTypes: [ new InstanceType('c5n.large') ],
        instanceRole: batchEcsInstanceProfile.attrArn,
        securityGroups: [ sg ],
      },
    });

    const jobQueue = new batch.JobQueue(this, 'JobQueue', {
      jobQueueName: Aws.STACK_NAME,
      computeEnvironments: [
        {
          computeEnvironment,
          order: 1,
        },
      ],
    });

    const jobRole = new iam.Role(this, 'JobRole', {
      assumedBy: new iam.ServicePrincipal('ecs-tasks.amazonaws.com'),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName('ReadOnlyAccess')
      ],
      inlinePolicies: {
        TerminateJob: new iam.PolicyDocument({
          statements: [
            new iam.PolicyStatement({
              actions: ["batch:TerminateJob"],
              resources: ["*"]
            }),
            new iam.PolicyStatement({
              actions: [
                "kafka-cluster:Connect",
                "kafka-cluster:DescribeCluster"
              ],
              resources: [`arn:${Aws.PARTITION}:kafka:${Aws.REGION}:${Aws.ACCOUNT_ID}:cluster/${kafka.clusterName}/*`]
            }),
            new iam.PolicyStatement({
              actions: [
                "kafka-cluster:*Topic*",
                "kafka-cluster:WriteData",
                "kafka-cluster:ReadData"
              ],
              resources: [`arn:${Aws.PARTITION}:kafka:${Aws.REGION}:${Aws.ACCOUNT_ID}:topic/${kafka.clusterName}/*`]
            }),
            new iam.PolicyStatement({
              actions: [
                "kafka-cluster:AlterGroup",
                "kafka-cluster:DescribeGroup"
              ],
              resources: [`arn:${Aws.PARTITION}:kafka:${Aws.REGION}:${Aws.ACCOUNT_ID}:group/${kafka.clusterName}/*`]
            })
          ]
        })
      }
    });


    const containerImage = ecs.ContainerImage.fromAsset('./docker');

    const performanceTestJobDefinition = new batch.JobDefinition(this, 'PerformanceTestJobDefinition', {
      container: {
        image: containerImage,
        vcpus: 2,
        memoryLimitMiB: 4900,
        logConfiguration: {
          logDriver: batch.LogDriver.AWSLOGS,
          options: {
            'awslogs-region': Aws.REGION,
            'awslogs-group': performanceTestLogGroup.logGroupName
          }
        },
        jobRole: jobRole
      },
    });

    const defaultJobDefinition = new batch.JobDefinition(this, 'DefaultJobDefinition', {
      container: {
        image: containerImage,
        vcpus: 2,
        memoryLimitMiB: 4900,
        logConfiguration: {
          logDriver: batch.LogDriver.AWSLOGS,
          options: {
            'awslogs-region': Aws.REGION,
            'awslogs-group': defaultLogGroup.logGroupName
          }
        },
        jobRole: jobRole
      },
    });



    const payload = TaskInput.fromObject({
      'num_jobs.$': 'States.JsonToString($.current_test.parameters.num_jobs)',                                //hack to convert number to string
      'replication_factor.$': 'States.JsonToString($.current_test.parameters.replication_factor)',
      topic_name: sfn.JsonPath.stringAt('$.current_test.parameters.topic_name'),
      depletion_topic_name: sfn.JsonPath.stringAt('$.current_test.parameters.depletion_topic_name'),
      'record_size_byte.$': 'States.JsonToString($.current_test.parameters.record_size_byte)',
      'records_per_sec.$': 'States.JsonToString($.current_test.parameters.records_per_sec)',
      'num_partitions.$': 'States.JsonToString($.current_test.parameters.num_partitions)',
      'duration_sec.$': 'States.JsonToString($.current_test.parameters.duration_sec)',
      producer_props: sfn.JsonPath.stringAt('$.current_test.parameters.client_props.producer'),             //this fails for empty strings :-(
      'num_producers.$':  'States.JsonToString($.current_test.parameters.num_producers)',
      'num_records_producer.$': 'States.JsonToString($.current_test.parameters.num_records_producer)',
      consumer_props: sfn.JsonPath.stringAt('$.current_test.parameters.client_props.consumer'),
      'num_consumer_groups.$': 'States.JsonToString($.current_test.parameters.consumer_groups.num_groups)',
      'size_consumer_group.$': 'States.JsonToString($.current_test.parameters.consumer_groups.size)',
      'num_records_consumer.$': 'States.JsonToString($.current_test.parameters.num_records_consumer)',
    });

    const commandParameters = [
      '--region', Aws.REGION,
      '--msk-cluster-arn', kafka.cfnCluster? kafka.cfnCluster.ref : '-',
      '--bootstrap-broker', 'bootstrapBrokerString' in props ? props.bootstrapBrokerString : '-',
      '--num-jobs', 'Ref::num_jobs',
      '--replication-factor', 'Ref::replication_factor',
      '--topic-name', 'Ref::topic_name',
      '--depletion-topic-name', 'Ref::depletion_topic_name',
      '--record-size-byte', 'Ref::record_size_byte',
      '--records-per-sec', 'Ref::records_per_sec',
      '--num-partitions', 'Ref::num_partitions',
      '--duration-sec', 'Ref::duration_sec',
      '--producer-props', 'Ref::producer_props',
      '--num-producers', 'Ref::num_producers',
      '--num-records-producer', 'Ref::num_records_producer',
      '--consumer-props', 'Ref::consumer_props',
      '--num-consumer-groups', 'Ref::num_consumer_groups',
      '--num-records-consumer', 'Ref::num_records_consumer',
      '--size-consumer-group', 'Ref::size_consumer_group'
    ]


    const queryProducerResultLambda = new lambda.Function(this, 'QueryProducerResultLambda', { 
      runtime: lambda.Runtime.NODEJS_14_X,
      code: lambda.Code.fromAsset('lambda'),
      timeout: Duration.minutes(2),
      handler: 'query-test-output.queryProducerOutput',
      environment: {
        LOG_GROUP_NAME: performanceTestLogGroup.logGroupName
      }
    });

    performanceTestLogGroup.grant(queryProducerResultLambda, 'logs:FilterLogEvents')

    queryProducerResultLambda.addToRolePolicy(
      new iam.PolicyStatement({
        actions: ['batch:DescribeJobs'],
        resources: ['*']
      })
    );

    const queryProducerResult = new tasks.LambdaInvoke(this, 'QueryProducerResult', {
      lambdaFunction: queryProducerResultLambda,
      resultPath: '$.producer_result'
    });

    // if the query restult is not available in cloudwatch yet, retry query
    queryProducerResult.addRetry({
      interval: Duration.seconds(10),
      maxAttempts: 3
    });


    const runPerformanceTest = new tasks.BatchSubmitJob(this, 'RunPerformanceTest', {
      jobName: 'RunPerformanceTest',
      jobDefinitionArn: performanceTestJobDefinition.jobDefinitionArn,
      jobQueueArn: jobQueue.jobQueueArn,
      arraySize: sfn.JsonPath.numberAt('$.current_test.parameters.num_jobs'),
      containerOverrides: {
        command: [...commandParameters, '--command', 'run-performance-test' ]
      },
      payload: payload,
      resultPath: '$.job_result',
    });

    runPerformanceTest.next(queryProducerResult);


    const depleteCreditsConstruct = new CreditDepletion(this, 'DepleteCreditsSfn', {
      clusterName: kafka.clusterName,
      jobDefinition: defaultJobDefinition,
      jobQueue: jobQueue,
      commandParameters: commandParameters,
      payload: payload        
    })

    const depleteCredits = new tasks.StepFunctionsStartExecution(this, 'DepleteCredits', {
      stateMachine: depleteCreditsConstruct.stateMachine,
      integrationPattern: IntegrationPattern.RUN_JOB,
      inputPath: '$',
      resultPath: 'DISCARD',
      timeout: Duration.hours(6)    //fixme: use timeoutPath when it becomes exposed through cdk
    });

    depleteCredits.next(runPerformanceTest);


    const createTopics = new tasks.BatchSubmitJob(this, 'CreateTopics', {
      jobName: 'CreateTopics',
      jobDefinitionArn: defaultJobDefinition.jobDefinitionArn,
      jobQueueArn: jobQueue.jobQueueArn,
      containerOverrides: {
        command: [...commandParameters, '--command', 'create-topics' ]
      },
      payload: payload,
      resultPath: 'DISCARD',
    });

    createTopics.next(depleteCredits);


    const finalState = new sfn.Succeed(this, 'Succeed');

    const checkCompleted = new sfn.Choice(this, 'CheckAllTestsCompleted')
      .when(sfn.Condition.isPresent('$.current_test'), createTopics)
      .otherwise(finalState);


    const updateTestParameterLambda = new lambda.Function(this, 'UpdateTestParameterLambda', {
      runtime: lambda.Runtime.PYTHON_3_8,
      code: lambda.Code.fromAsset('lambda'),
      timeout: Duration.seconds(5),
      handler: 'test-parameters.increment_index_and_update_parameters',
    });

    const updateTestParameter = new tasks.LambdaInvoke(this, 'UpdateTestParameter', {
      lambdaFunction: updateTestParameterLambda,
      outputPath: '$.Payload',
    });

    updateTestParameter.next(checkCompleted);


    const deleteTopics = new tasks.BatchSubmitJob(this, 'DeleteAllTopics', {
      jobName: 'DeleteAllTopics',
      jobDefinitionArn: defaultJobDefinition.jobDefinitionArn,
      jobQueueArn: jobQueue.jobQueueArn,
      containerOverrides: {
        command: [...commandParameters, '--command', 'delete-topics' ]
      },
      payload: payload,
      resultPath: 'DISCARD',
    });

    deleteTopics.next(updateTestParameter);
    queryProducerResult.next(deleteTopics);


    const fail = new sfn.Fail(this, 'Fail');


    const cleanup = new tasks.BatchSubmitJob(this, 'DeleteAllTopicsTrap', {
      jobName: 'DeleteAllTopics',
      jobDefinitionArn: defaultJobDefinition.jobDefinitionArn,
      jobQueueArn: jobQueue.jobQueueArn,
      containerOverrides: {
        command: [ 
          '--region', Aws.REGION, 
          '--msk-cluster-arn', kafka.cfnCluster? kafka.cfnCluster.ref : '-', 
          '--bootstrap-broker', 'bootstrapBrokerString' in props ? props.bootstrapBrokerString : '-',
          '--producer-props', 'Ref::producer_props',
          '--command', 'delete-topics' 
        ]
      },
      payload: TaskInput.fromObject({
        'producer_props.$': '$.Cause'
      }),
      resultPath: 'DISCARD'
    });

    cleanup.next(fail);


    const parallel = new sfn.Parallel(this, 'IterateThroughPerformanceTests');

    parallel.branch(updateTestParameter);
    parallel.addCatch(cleanup);

    const stateMachine = new sfn.StateMachine(this, 'StateMachine', {
      definition: parallel,
      stateMachineName: `${Aws.STACK_NAME}-main`
    });

    if (props.initialPerformanceTest) {
      new AwsCustomResource(this, 'InitialPerformanceTestResource', {
        policy: custom_resources.AwsCustomResourcePolicy.fromSdkCalls({
          resources: [ stateMachine.stateMachineArn ]
        }),
        onCreate: {
          action: "startExecution",
          service: "StepFunctions",
          parameters: {
            input: JSON.stringify(props.initialPerformanceTest),
            stateMachineArn: stateMachine.stateMachineArn
          },
          physicalResourceId: {
            id: 'InitialPerformanceTestCall'
          }
        }
      })
    }


    const bytesInWidget = new cloudwatch.GraphWidget({
      left: [
        new cloudwatch.MathExpression({
          expression: `SUM(SEARCH('{AWS/Kafka,"Broker ID","Cluster Name"} "Cluster Name"="${kafka.clusterName}" MetricName="BytesInPerSec"', 'Minimum', 60))/1024/1024`,
          label: `cluster ${kafka.clusterName}`,
          usingMetrics: {}
        }),
        new cloudwatch.MathExpression({
          expression: `SEARCH('{AWS/Kafka,"Broker ID","Cluster Name"} "Cluster Name"="${kafka.clusterName}" MetricName="BytesInPerSec"', 'Minimum', 60)/1024/1024`,
          label: 'broker',
          usingMetrics: {}
        }),
      ],
      leftYAxis: {
        min: 0
      },
      title: 'throughput in (MB/sec)',
      width: 24,
      liveData: true
    });

    const bytesInOutStddevWidget = new cloudwatch.GraphWidget({
      left: [
        new cloudwatch.MathExpression({
          expression: `STDDEV(SEARCH('{AWS/Kafka,"Broker ID","Cluster Name"} "Cluster Name"="${kafka.clusterName}" MetricName="BytesInPerSec"', 'Minimum', 60))/1024/1024`,
          label: 'in',
          usingMetrics: {}
        }),
        new cloudwatch.MathExpression({
          expression: `STDDEV(SEARCH('{AWS/Kafka,"Broker ID","Cluster Name"} "Cluster Name"="${kafka.clusterName}" MetricName="BytesOutPerSec"', 'Minimum', 60))/1024/1024`,
          label: 'out',
          usingMetrics: {}
        }),
      ],
      leftYAxis: {
        min: 0,
        max: 3
      },
      leftAnnotations: [{
        value: 0.75
      }],
      title: 'stdev broker throughput (MB/sec)',
      width: 24,
      liveData: true,
    });


    const bytesOutWidget = new cloudwatch.GraphWidget({
      left: [
        new cloudwatch.MathExpression({
          expression: `SUM(SEARCH('{AWS/Kafka,"Broker ID","Cluster Name"} "Cluster Name"="${kafka.clusterName}" MetricName="BytesOutPerSec"', 'Average', 60))/1024/1024`,
          label: `cluster ${kafka.clusterName}`,
          usingMetrics: {}
        }),
        new cloudwatch.MathExpression({
          expression: `SEARCH('{AWS/Kafka,"Broker ID","Cluster Name"} "Cluster Name"="${kafka.clusterName}" MetricName="BytesOutPerSec"', 'Average', 60)/1024/1024`,
          label: 'broker',
          usingMetrics: {}
        }),
      ],
      title: 'throughput out (MB/sec)',
      width: 24,
      liveData: true
    });

    const burstBalance = new cloudwatch.GraphWidget({
      left: [
        new cloudwatch.MathExpression({
          expression: `SEARCH('{AWS/Kafka,"Broker ID","Cluster Name"} "Cluster Name"="${kafka.clusterName}" MetricName="BurstBalance"', 'Minimum', 60)`,
          label: `EBS volume BurstBalance`,
          usingMetrics: {}
        }),
        new cloudwatch.MathExpression({
          expression: `SEARCH('{AWS/Kafka,"Broker ID","Cluster Name"} "Cluster Name"="${kafka.clusterName}" MetricName="CPUCreditBalance"', 'Minimum', 60)`,
          label: `CPUCreditBalance`,
          usingMetrics: {}
        }),
      ],
      title: 'burst balances',
      width: 24,
      liveData: true
    });


    const trafficShaping = new cloudwatch.GraphWidget({
      left: [
        new cloudwatch.MathExpression({
          expression: `SEARCH('{AWS/Kafka,"Broker ID","Cluster Name"} "Cluster Name"="${kafka.clusterName}" MetricName="BwInAllowanceExceeded"', 'Maximum', 60)`,
          label: `BwInAllowanceExceeded`,
          usingMetrics: {}
        }),
        new cloudwatch.MathExpression({
          expression: `SEARCH('{AWS/Kafka,"Broker ID","Cluster Name"} "Cluster Name"="${kafka.clusterName}" MetricName="BwOutAllowanceExceeded"', 'Maximum', 60)`,
          label: `BwOutAllowanceExceeded`,
          usingMetrics: {}
        }),
        new cloudwatch.MathExpression({
          expression: `SEARCH('{AWS/Kafka,"Broker ID","Cluster Name"} "Cluster Name"="${kafka.clusterName}" MetricName="PpsAllowanceExceeded"', 'Maximum', 60)`,
          label: `PpsAllowanceExceeded`,
          usingMetrics: {}
        }),
        new cloudwatch.MathExpression({
          expression: `SEARCH('{AWS/Kafka,"Broker ID","Cluster Name"} "Cluster Name"="${kafka.clusterName}" MetricName="ConntrackAllowanceExceeded"', 'Maximum', 60)`,
          label: `ConntrackAllowanceExceeded`,
          usingMetrics: {}
        }),
      ],
      title: 'traffic shaping applied',
      width: 24,
      liveData: true
    });


    const cpuIdleWidget = new cloudwatch.GraphWidget({
      left: [
        new cloudwatch.MathExpression({
          expression: `AVG(SEARCH('{AWS/Kafka,"Broker ID","Cluster Name"} "Cluster Name"="${kafka.clusterName}" MetricName="CpuIdle"', 'Minimum', 60))`,
          label: `cluster ${kafka.clusterName}`,
          usingMetrics: {}
        }),
        new cloudwatch.MathExpression({
          expression: `SEARCH('{AWS/Kafka,"Broker ID","Cluster Name"} "Cluster Name"="${kafka.clusterName}" MetricName="CpuIdle"', 'Minimum', 60)`,
          label: 'broker',
          usingMetrics: {}
        }),
      ],
      title: 'cpu idle (percent)',
      width: 24,
      leftYAxis: {
        min: 0
      },
      liveData: true
    });

    const replicationWidget = new cloudwatch.GraphWidget({
      left: [
        new cloudwatch.MathExpression({
          expression: `SUM(SEARCH('{AWS/Kafka,"Broker ID","Cluster Name"} "Cluster Name"="${kafka.clusterName}" MetricName="UnderReplicatedPartitions"', 'Maximum', 60))`,
          label: 'under replicated',
          usingMetrics: {}
        }),
        new cloudwatch.MathExpression({
          expression: `SUM(SEARCH('{AWS/Kafka,"Broker ID","Cluster Name"} "Cluster Name"="${kafka.clusterName}" MetricName="UnderMinIsrPartitionCount"', 'Maximum', 60))`,
          label: 'under min isr',
          usingMetrics: {}
        }),
      ],
      leftYAxis: {
        min: 0
      },
      title: 'partition (count)',
      width: 24,
      liveData: true
    });

    const partitionWidget = new cloudwatch.GraphWidget({
      left: [
        new cloudwatch.MathExpression({
          expression: `SUM(SEARCH('{AWS/Kafka,"Broker ID","Cluster Name"} "Cluster Name"="${kafka.clusterName}" MetricName="PartitionCount"', 'Maximum', 60))`,
          label: 'partiton count',
          usingMetrics: {}
        }),
        new cloudwatch.MathExpression({
          expression: `SEARCH('{AWS/Kafka,"Broker ID","Cluster Name"} "Cluster Name"="${kafka.clusterName}" MetricName="PartitionCount"', 'Maximum', 60)`,
          label: 'partiton count',
          usingMetrics: {}
        }),        
      ],
      leftYAxis: {
        min: 0
      },
      title: 'partition (count)',
      width: 24,
      liveData: true
    });

    const dashboard = new cloudwatch.Dashboard(this, 'Dashboard', {
      dashboardName: Aws.STACK_NAME,
      widgets: [
        [ bytesInWidget ],
        [ bytesOutWidget ],
        [ trafficShaping ],
        [ burstBalance ],
        [ bytesInOutStddevWidget ],
        [ cpuIdleWidget ],
        [ replicationWidget ],
        [ partitionWidget ]
      ],
    });

    const sagemakerInstanceRole = new iam.Role(this, 'SagemakerInstanceRole', {
      assumedBy: new iam.ServicePrincipal('sagemaker.amazonaws.com'),
      managedPolicies: [
//        iam.ManagedPolicy.fromAwsManagedPolicyName('CloudWatchReadOnlyAccess'),
      ],
      inlinePolicies: {
        cloudFormation: new iam.PolicyDocument({
          statements: [new iam.PolicyStatement({
            actions: [
              'cloudformation:DescribeStacks'
            ],
            resources: [ Aws.STACK_ID ],
          })]
        }),
        cwLogs: new iam.PolicyDocument({
          statements: [
            new iam.PolicyStatement({
              actions: [
                'logs:StartQuery'
              ],
              resources: [ performanceTestLogGroup.logGroupArn ],
            }),
            new iam.PolicyStatement({
              actions: [
                'logs:GetQueryResults'
              ],
              resources: [ '*' ],
            })
          ]
        }),
        stepFunctions: new iam.PolicyDocument({
          statements: [new iam.PolicyStatement({
            actions: [
              'states:DescribeExecution'
            ],
            resources: [ `arn:${Aws.PARTITION}:states:${Aws.REGION}:${Aws.ACCOUNT_ID}:execution:${stateMachine.stateMachineName}:*` ],
          })]
        })
      }
    });

    const sagemakerNotebook = new sagemaker.CfnNotebookInstance(this, 'SagemakerNotebookInstance', {
      instanceType: 'ml.t3.medium',
      roleArn: sagemakerInstanceRole.roleArn,
      notebookInstanceName: Aws.STACK_NAME,
      defaultCodeRepository: 'https://github.com/aws-samples/performance-testing-framework-for-apache-kafka/'
    });

    new CfnOutput(this, 'SagemakerNotebook', { 
      value: `https://console.aws.amazon.com/sagemaker/home#/notebook-instances/openNotebook/${sagemakerNotebook.attrNotebookInstanceName}?view=lab`
    });

    new CfnOutput(this, 'LogGroupName', { 
      value: performanceTestLogGroup.logGroupName 
    });
  }
}
