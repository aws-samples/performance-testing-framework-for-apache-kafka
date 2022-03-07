# Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# 
# Permission is hereby granted, free of charge, to any person obtaining a copy of
# this software and associated documentation files (the "Software"), to deal in
# the Software without restriction, including without limitation the rights to
# use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
# the Software, and to permit persons to whom the Software is furnished to do so.
# 
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
# FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
# COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
# IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
# CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

import re
import json
import datetime
import collections

def get_test_details(test_params, stepfunctions, cloudformation):
    sfn_status = []
    test_details = []

    for test_param in test_params:
        if 'execution_arn' not in test_param:
            # if the execution_arn is not included, we cannot query them and just assume that the details are complete and correct
            test_details.append(test_param)
            continue

        execution_arn = test_param['execution_arn'].strip()
        execution_details = stepfunctions.describe_execution(executionArn=execution_arn)

        if 'stopDate' in execution_details:
            stop_date = execution_details['stopDate']
        else:
            # if the workflow is still running, just query everything up until now
            stop_date = datetime.datetime.now()
                
        # the name of the state machine includes the CFN stack name 
        stack_name = test_param['execution_arn'].split(':')[-2].split('-main')[0]
        
        # obtain the name of the log group for containing all test results from cloud watch
        stack_details = cloudformation.describe_stacks(StackName=stack_name)
        log_group_name = next(filter(lambda x: x['OutputKey']=='LogGroupName', stack_details['Stacks'][0]['Outputs']))['OutputValue']

        # extract the cluster parameters from the stack name
        cluster_properties_p = re.compile(r'(\S+)--(\d+)-([a-zA-Z0-9]+)-(\d+)-([tf])-(\d+)(?:-(\d+))?.*')
        cluster_properties_m = cluster_properties_p.search(stack_name)
        
        cluster_properties = {
            'cluster_name': stack_name if cluster_properties_m else "n/a",
            'cluster_id': cluster_properties_m.group(1) if cluster_properties_m else "n/a",
            'num_brokers': int(cluster_properties_m.group(2)) if cluster_properties_m else "n/a",
            'broker_type': cluster_properties_m.group(3) if cluster_properties_m else "n/a",
            'broker_storage': int(cluster_properties_m.group(4)) if cluster_properties_m else "n/a",
            'in_cluster_encryption': cluster_properties_m.group(5) if cluster_properties_m else "n/a",
            'kafka_version': cluster_properties_m.group(6) if cluster_properties_m else "n/a",
            'provisioned_throughput': int(cluster_properties_m.group(7)) if cluster_properties_m and cluster_properties_m.group(7) is not None else 0,
        }
        
        status = {
            'log_group_name': log_group_name,
            'start_date': execution_details['startDate'],
            'stop_date': stop_date,
            'test_parameters': json.loads(execution_details['input'])['test_specification']['parameters'],
            'cluster_properties': cluster_properties
        }

        # if the test is still running, add the execution arn, so that the details can be queried again
        if execution_details['status'] == 'RUNNING':
            status['execution_arn'] = test_param['execution_arn'] 

        test_details.append(status)
        sfn_status.append(execution_details['status'])

    if len(sfn_status) > 0:
        print("sfn workflow status: ", dict(collections.Counter(sfn_status)))
        print()
        for test_detail in test_details:
            print("test_params.extend([", test_detail, "])")
            print()

    return test_details