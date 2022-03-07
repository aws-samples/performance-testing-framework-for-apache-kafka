## Setup CDK Environment

To deploy the CDK template, you need to have node, cdk, and docker configured locally. Alternatively, you can use the `amzn2-ami-ecs-hvm-2.0.20210106-x86_64-ebs` AMI with an Amazon EC2 instance and the following steps to configure an environment that satisfies these constraints.

```bash
# Install Node Version Manager
curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.34.0/install.sh | bash
. ~/.nvm/nvm.sh

# Install Node and CDK
nvm install node
npm -g install typescript
npm install -g aws-cdk

# Bootstrap CDK envitonment, if you have never executed CDK in this AWS account and region.
cdk bootstrap

sudo yum install -y git awscli
```