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
import { Aws, Stack, StackProps } from 'aws-cdk-lib';
import { aws_ecs as ecs } from 'aws-cdk-lib';
import { aws_ec2 as ec2 } from 'aws-cdk-lib';
import { aws_iam as iam } from 'aws-cdk-lib';
import { aws_imagebuilder as imagebuilder } from 'aws-cdk-lib';


export class BatchAmiStack extends Stack {
  ami: ec2.IMachineImage;

  constructor(scope: Construct, id: string, props?: StackProps) {
    super(scope, id, props);


    const agentComponent = new imagebuilder.CfnComponent(this, 'ImageBuilderComponent', {
      name: `${Aws.STACK_NAME}`,
      platform: 'Linux',
      version: '0.1.0',
      data: `name: Custom image for AWS Batch
description: Builds a custom image for AWS with additional debugging tools
schemaVersion: 1.0

phases:
  - name: build
    steps:
      - name: InstallCloudWatchAgentStep
        action: ExecuteBash
        inputs:
          commands:
            - |
              cat <<'EOF' >> /etc/ecs/ecs.config
              ECS_ENABLE_CONTAINER_METADATA=true
              EOF
              - rpm -Uvh https://amazoncloudwatch-agent-us-west-2.s3.us-west-2.amazonaws.com/amazon_linux/amd64/latest/amazon-cloudwatch-agent.rpm
            - |
              cat > /opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json << 'EOF'
              {
                "agent":{
                    "metrics_collection_interval":10,
                    "omit_hostname":true
                },
                "logs":{
                    "logs_collected":{
                      "files":{
                          "collect_list":[
                            {
                                "file_path":"/var/log/ecs/ecs-agent.log*",
                                "log_group_name":"/aws-batch/ecs-agent.log",
                                "log_stream_name":"{instance_id}",
                                "timezone":"Local"
                            },
                            {
                                "file_path":"/var/log/ecs/ecs-init.log",
                                "log_group_name":"/aws-batch/ecs-init.log",
                                "log_stream_name":"{instance_id}",
                                "timezone":"Local"
                            },
                            {
                                "file_path":"/var/log/ecs/audit.log*",
                                "log_group_name":"/aws-batch/audit.log",
                                "log_stream_name":"{instance_id}",
                                "timezone":"Local"
                            },
                            {
                                "file_path":"/var/log/messages",
                                "log_group_name":"/aws-batch/messages",
                                "log_stream_name":"{instance_id}",
                                "timezone":"Local"
                            }
                          ]
                      }
                    }
                }
              }
              EOF
            - /opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent-ctl -a fetch-config -m ec2 -c file:/opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json -s`
    });

    const imageReceipe = new imagebuilder.CfnImageRecipe(this, 'ImageBuilderReceipt', {
      components: [{
        componentArn: agentComponent.attrArn
      }],
      name: Aws.STACK_NAME,
      parentImage: ecs.EcsOptimizedImage.amazonLinux2().getImage(this).imageId,
      version: '0.1.0'
    });

    const imageBuilderRole = new iam.Role(this, 'ImageBuilderRole', {
      assumedBy: new iam.ServicePrincipal('ec2.amazonaws.com'),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName('AmazonSSMManagedInstanceCore'),
        iam.ManagedPolicy.fromAwsManagedPolicyName('EC2InstanceProfileForImageBuilder')
      ]
    });

    const imageBuilderProfile = new iam.CfnInstanceProfile(this, 'ImageBuilderInstanceProfile', {
      roles: [ imageBuilderRole.roleName ],
    });

    const infrastructureconfiguration = new imagebuilder.CfnInfrastructureConfiguration(this, 'InfrastructureConfiguration', {
      name: Aws.STACK_NAME,
      instanceProfileName: imageBuilderProfile.ref
    });

    const image = new imagebuilder.CfnImage(this, 'Image', {
      imageRecipeArn: imageReceipe.attrArn,
      infrastructureConfigurationArn: infrastructureconfiguration.attrArn,
    });

    this.ami = ec2.MachineImage.genericLinux({
      [ this.region ] : image.attrImageId
    });
  }
}