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
import pprint
import itertools
import numpy as np
from more_itertools import ilen
from statistics import mean, median, variance, stdev
from dateutil.tz import tzlocal


producer_aggregation_fns = [
#    ('sent_records', sum),
#    ('sent_records_sec', min),
#    ('sent_records_sec', sum),
    ('sent_mb_sec', min),
#    ('sent_mb_sec', sum),
    ('latency_ms_avg', mean),
    ('latency_ms_avg', stdev),
    ('latency_ms_p50', mean),
    ('latency_ms_p50', stdev),
    ('latency_ms_p95', mean),
    ('latency_ms_p95', stdev),
    ('latency_ms_p99', mean),
    ('latency_ms_p99', stdev),
    ('latency_ms_p999', mean),
    ('latency_ms_p999', stdev),
    ('latency_ms_max', max),
    ('latency_ms_max', stdev),
    ('actual_duration_div_requested_duration_sec', max),
    ('start_ts', min),
    ('end_ts', max)
]

consumer_aggregation_fns = [
    ('consumed_mb', sum),
    ('consumed_mb_sec', sum),
    ('consumed_records', sum),
    ('records_lag_max', max),
    ('actual_duration_div_requested_duration_sec', max),
    ('start_ts', min),
    ('end_ts', max)
]

def broker_type_to_num(broker):
    if "m5large" in broker:
        return 0
    elif "m5xlarge" in broker:
        return 1
    else:
        return int(re.search('m5(\d+)xlarge', broker).group(1))

# aggregate intividual producer/consumer results into one for identical test parameters

def aggregate_cw_logs(producer_stats, consumer_stats, partitons):
    producer_aggregated_stats = []
    consumer_aggregated_stats = []
        
    partiton_by_fn = lambda x: tuple([v for k,v in x['test_params'].items() if k not in partitons['ignore_keys']])

    for params,group in itertools.groupby(sorted(producer_stats, key=partiton_by_fn), key=partiton_by_fn):
        # convert iterator to list, as we need to iterate over it several times
        lst = list(group)
        
        # aggregate individual test results into a single result
        agg_test_results = {
            "{}_{}".format(attr, aggregation_fn.__name__): aggregation_fn(map(lambda x: x['test_results'][attr], lst))
            for (attr,aggregation_fn) in producer_aggregation_fns
        }
        
        # all test params within a group are identical, so just pick the first one
        test_params = lst[0]['test_params']
        
        producer_aggregated_stats.append({
            'test_params': {
                **test_params,
                'brokers' : "{} {}".format(test_params['num_brokers'], test_params['broker_type']),
                'brokers_type_numeric' : broker_type_to_num(test_params['broker_type'])
            },
            'test_results': {
                **agg_test_results,
                'num_tests': len(lst) / test_params['num_producers'],                
                'sent_div_requested_mb_per_sec' : agg_test_results['sent_mb_sec_min'] / (test_params['cluster_throughput_mb_per_sec']/test_params['num_producers'])
            }
        })


    for params,group in itertools.groupby(sorted(consumer_stats, key=partiton_by_fn), key=partiton_by_fn):
        # convert iterator to list, as we need to iterate over it several times
        lst = list(group)

        # aggregate individual test results into a single result
        agg_test_results = {
            "{}_{}".format(attr, aggregation_fn.__name__): aggregation_fn(map(lambda x: x['test_results'][attr], lst))
            for (attr,aggregation_fn) in consumer_aggregation_fns
        }

        # all test params within a group are identical, so just pick the first one
        test_params = lst[0]['test_params']

        consumer_aggregated_stats.append({
            'test_params': {
                **test_params,
                'brokers' : "{} {}".format(test_params['num_brokers'], test_params['broker_type']),
            },
            'test_results': {
                **agg_test_results,
                'num_tests': len(lst) / (test_params['consumer_groups.num_groups'] * test_params['consumer_groups.size']),
            }
        })


    combined_stats = []

    return (producer_aggregated_stats, consumer_aggregated_stats, combined_stats)
