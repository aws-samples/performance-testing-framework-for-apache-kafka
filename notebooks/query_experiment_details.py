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
import sys
import time
import itertools
import collections
import dateutil.parser
from flatten_dict import flatten
from collections import defaultdict


cwlogs_query_limit = 4

def obtain_raw_logs(test_details, cloudwatch_logs):
    # query cw logs to get the raw logs during the period the step functions workflow was running
    # cw logs limits the number of concurrent queries, so the number of running queries always needs to be lower or equal cwlogs_query_limit
    
    # filter queries that are currently running (query metadata is added to the cwlogs_query in the test details once they are submitted)
    incomplete_query_results_iter = lambda: filter(lambda x: 'cwlogs_query' in x and ('cwlogs_query_result' not in x or x['cwlogs_query_result']['status'] == 'Running'), test_details)
    
    # print summary of running, completed, and failed queries
    output_statistics = lambda: print("query status:", dict(collections.Counter(map(lambda x: x['cwlogs_query_result']['status'] if 'cwlogs_query_result' in x else 'Pending', test_details))))

    # populate queue with all queries that need to be executed
    queue = test_details.copy()
    cwlogs_query_capacity = cwlogs_query_limit

    output_statistics()

    # while there are queries to run and we can still run queries
    while len(queue)>0 or cwlogs_query_capacity<cwlogs_query_limit:
        
        # submit cwlogs_query_capacity additional queries from the queue to cloudwatch logs
        for test_detail in queue[:cwlogs_query_capacity]:
            test_detail['cwlogs_query'] = cloudwatch_logs.start_query(
                logGroupName=test_detail['log_group_name'],
                startTime=int(test_detail['start_date'].timestamp()),
                endTime=int(test_detail['stop_date'].timestamp()),
                queryString='fields @timestamp, @message, @logStream | filter @message like /(kafka-producer-perf-test.sh --topic|^\d+ records sent.*99\.9th|kafka-consumer-perf-test.sh --topic|consumer-fetch-manager-metrics:(records-consumed-total|records-consumed-rate|bytes-consumed-rate|bytes-consumed-total|records-lag-max):\{\S+\})/ | sort @timestamp asc | limit 10000'
            )

        # remove started queries from the queue
        del queue[:cwlogs_query_capacity]

        time.sleep(2)

        # obtain the query result for completed queries
        for test_detail in incomplete_query_results_iter():
            test_detail['cwlogs_query_result'] = cloudwatch_logs.get_query_results(
                queryId=test_detail['cwlogs_query']['queryId']
            )

        # adapt query capacity of cloudwatch based on the running queries 
        cwlogs_query_capacity = cwlogs_query_limit - len(list(incomplete_query_results_iter()))

        output_statistics()

        

key_by_logstream_fn = lambda x: x['@logStream']

producer_result_p = re.compile(r'(\S+) records sent, (\S+) records.sec \((\S+) MB.sec\), (\S+) ms avg latency, (\S+) ms max latency, (\S+) ms 50th, (\S+) ms 95th, (\S+) ms 99th, (\S+) ms 99.9th')
consumer_result_p = re.compile(r'(\S+)$')
consumer_timeout_p = re.compile(r'--timeout (\S+)')

def query_cw_logs(test_details, cloudwatch_logs):
    # first, obtain the raw output from cloudwatch
    # then, parse the output so that each producer/consumer result is clearly mapped to the test parameters that lead to the result
    # this will still give multiple results per test, one result per producer/consumer

    obtain_raw_logs(test_details, cloudwatch_logs)
        
    producer_stats = []
    consumer_stats = []
    
    max_topic_property_length = 15

    for raw_test_result in test_details:
        # create regular expressions that match the test parameters indexes in the topic name
        test_params_re = { name: re.compile(r'{}\S*?-(\d+)'.format(name.replace('_', '-')[:max_topic_property_length])) for name in raw_test_result['test_parameters'].keys()}
        test_params_re = {
            **test_params_re,
            'test_id' : re.compile(r'--topic test-id-(\d+)--throughput-series-id-\d+'),
            'throughput_series_id' : re.compile(r'--topic test-id-\d+--throughput-series-id-(\d+)')
        }
        
        # transform the cloudwatch logs output into a dictionary with properly named keys
        statistics_result = map(lambda x: {kv['field']: kv['value'] for kv in x}, raw_test_result['cwlogs_query_result']['results'])

        # groups the results by log stream name so that each producer/consumer output can be transformed independent of the others
        for log_stream,logs in itertools.groupby(sorted(statistics_result, key=key_by_logstream_fn), key=key_by_logstream_fn):
            params, *results = list(logs)

            if len(results)==0:
                continue
            
            start_ts = dateutil.parser.parse(params['@timestamp'])
            end_ts = max(map(lambda x: dateutil.parser.parse(x['@timestamp']), results))
            duration_sec = (end_ts - start_ts).total_seconds()

            # extract the test parameter indexes from the topic name
            test_index = { name: int(re.search(params['@message']).group(1)) for name, re in test_params_re.items()}
            
            # lookup index to actual parameter value
            test_params = { 
                name: raw_test_result['test_parameters'][name][index] if name in raw_test_result['test_parameters'] else index 
                for name, index in test_index.items()
            }
            
            # parse producer & consumer probs into a dict
            test_params['producer'] =  dict([arr.split('=') for arr in test_params['client_props']['producer'].split()])
            test_params['consumer'] =  dict([arr.split('=') for arr in test_params['client_props']['consumer'].split()])

            
            if "kafka-producer-perf-test" in params['@message']:            
                # last matched output contains the overall statistics of the producer command
                producer_result_m = producer_result_p.search(results[-1]['@message'])

                test_results = {
                    'sent_records': float(producer_result_m.group(1)),
                    'sent_records_sec': float(producer_result_m.group(2)),
                    'sent_mb_sec': float(producer_result_m.group(3)),
                    'latency_ms_avg': float(producer_result_m.group(4)),
                    'latency_ms_max': float(producer_result_m.group(5)),
                    'latency_ms_p50': float(producer_result_m.group(6)),
                    'latency_ms_p95': float(producer_result_m.group(7)),
                    'latency_ms_p99': float(producer_result_m.group(8)),
                    'latency_ms_p999': float(producer_result_m.group(9)),
                    'log_group': raw_test_result['log_group_name'],
                    'log_stream': log_stream,
                    'start_ts': start_ts,
                    'end_ts': end_ts,
                    'actual_duration_div_requested_duration_sec': duration_sec/test_params['duration_sec']
                }

                producer_stats.append({
                    'test_params': {
                        **flatten(test_params, reducer='dot'),
                        **raw_test_result['cluster_properties']
                    },
                    'test_results': test_results
                })
            elif "kafka-consumer-perf-test" in params['@message']:
                consumer_timeout_m = consumer_timeout_p.search(params['@message'])
                
                try:
                    metrics = {
                        metric: float(consumer_result_p.search(next(filter(lambda x: metric in x['@message'], results))['@message']).group(0))
                        for metric in ['records-consumed-total', 'records-consumed-rate', 'bytes-consumed-rate', 'bytes-consumed-total', 'records-lag-max']
                    }
                except StopIteration:
                    print("skipping incomplete result for test " + str(flatten(test_params, reducer='dot')), file=sys.stderr)

                test_results = {
                    'consumed_mb': metrics['bytes-consumed-total']/1024**2,
                    'consumed_mb_sec': metrics['bytes-consumed-rate']/1024**2,
                    'consumed_records':  metrics['records-consumed-total'],
                    'consumed_records_sec': metrics['records-consumed-rate'],
                    'records_lag_max': metrics['records-lag-max'],
                    'log_group': raw_test_result['log_group_name'],
                    'log_stream': results[-1]['@logStream'],
                    'start_ts': start_ts,
                    'end_ts': end_ts,
                    'actual_duration_div_requested_duration_sec': duration_sec/(test_params['duration_sec']+int(consumer_timeout_m.group(1))/1000)
                }

                consumer_stats.append({
                    'test_params': {**flatten(test_params, reducer='dot'), **raw_test_result['cluster_properties']},
                    'test_results': test_results
                })
    return (producer_stats, consumer_stats)