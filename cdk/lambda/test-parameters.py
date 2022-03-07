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
import os
import math
import random
import sys

random.seed()

def increment_index_and_update_parameters(event, context):
    test_specification = event["test_specification"]

    if "current_test" in event:
        previous_test_index = event["current_test"]["index"]
        skip_condition = event["test_specification"]["skip_remaining_throughput"]
        
        try:
            if evaluate_skip_condition(skip_condition, event):
                # if the skip condition matches, reset throughput index to 0 and increment next (hardcoded to first) index
                (updated_test_index, throughput_series_id) = skip_remaining_throughput(previous_test_index, event)
                
            else:
                (updated_test_index, throughput_series_id) = increment_test_index(previous_test_index, event, 0)
        except OverflowError:
            # all tests have been executed
            updated_test_index = None
            throughput_series_id = random.randint(0,100000)
    else:
        # if no test has been executed yet, start with the first index
        updated_test_index = {key: 0 for key in test_specification["parameters"].keys()}
        throughput_series_id = random.randint(0,100000)

    return update_parameters(test_specification, updated_test_index, random.randint(0,100000), throughput_series_id)



def evaluate_skip_condition(condition, event):
    if isinstance(condition, dict):
        if "greater-than" in condition:
            args = condition["greater-than"]

            return evaluate_skip_condition(args[0], event) > evaluate_skip_condition(args[1], event)
        elif "less-than" in condition:
            args = condition["less-than"]

            return evaluate_skip_condition(args[0], event) < evaluate_skip_condition(args[1], event)
        else:
            raise Exception("unable to parse condition: " + condition)
    elif isinstance(condition, str):
        if condition == "sent_div_requested_mb_per_sec":
            return event["producer_result"]["Payload"]["mbPerSecSum"] / event["current_test"]["parameters"]["cluster_throughput_mb_per_sec"]
        else:
            raise Exception("unable to parse condition: " + condition)
    elif isinstance(condition, int) or isinstance(condition, float):
        return condition
    else:
        raise Exception("unable to parse condition: " + condition)


def increment_test_index(index, event, pos):
    parameters = [key for key in event["test_specification"]["parameters"].keys()]

    # if all options to increment the index are exhausted
    if not (0 <= pos < len(parameters)):
        raise OverflowError

    paramater_name = parameters[pos]
    parameter_value = event["current_test"]["index"][paramater_name]

    if (parameter_value+1 < len(event["test_specification"]["parameters"][paramater_name])):
        # if there are still values that need to be tested for this parameter, increment the index
        return ({**index, paramater_name: parameter_value+1}, event["current_test"]["parameters"]["throughput_series_id"])
    else:
        if paramater_name == "cluster_throughput_mb_per_sec":
            # if the cluster throughput is exhausted, increment the next index and update the throughput series id
            (updated_index, _) = increment_test_index({**index, paramater_name: 0}, event, pos+1)
            updated_throughput_series_id = random.randint(0,100000)
            
            return (updated_index, updated_throughput_series_id)
        else:
            return increment_test_index({**index, paramater_name: 0}, event, pos+1)



def update_parameters(test_specification, updated_test_index, test_id, throughput_series_id):
    if updated_test_index is None:
        # all test have been completed
        return {
            "test_specification": test_specification
        }
    else:
        # get the new parameters for the updated index
        updated_parameters = {
            index_name: test_specification["parameters"][index_name][index_value] for (index_name,index_value) in updated_test_index.items()
        }

        # encode the current index into the topic name, so that the parameters can be extracted later
        max_topic_property_length = 15
        remove_invalid_chars = lambda x: re.sub(r'[^a-zA-Z0-9-]', '-', x.replace(' ', '-'))

        topic_name = 'test-id-' + str(test_id) + '--' + 'throughput-series-id-' + str(throughput_series_id) + '--' + '--'.join([f"{remove_invalid_chars(k)[:max_topic_property_length]}-{str(v)}" for k,v in updated_test_index.items()])
        depletion_topic_name = 'test-id-' + str(test_id) + '--' + 'throughput-series-id-' + str(throughput_series_id)  + '--depletion'

        record_size_byte = updated_parameters["record_size_byte"]

        if updated_parameters["cluster_throughput_mb_per_sec"]>0:
            producer_throughput_byte = updated_parameters["cluster_throughput_mb_per_sec"] * 1024**2 // updated_parameters["num_producers"]
            # one consumer group consumes the entire cluster throughput
            consumer_throughput_byte = updated_parameters["cluster_throughput_mb_per_sec"] * 1024**2 // updated_parameters["consumer_groups"]["size"] if updated_parameters["consumer_groups"]["size"] > 0 else 0

            num_records_producer = producer_throughput_byte * updated_parameters["duration_sec"] // record_size_byte
            num_records_consumer = consumer_throughput_byte * updated_parameters["duration_sec"] // record_size_byte
        else:
            # for depletion, publish messages as fast as possible
            producer_throughput_byte = -1
            consumer_throughput_byte = -1
            num_records_producer = 2147483647
            num_records_consumer = 2147483647

        # derive additional parameters for convenience
        updated_parameters = {
            **updated_parameters,
            "num_jobs": updated_parameters["num_producers"] + updated_parameters["consumer_groups"]["num_groups"]*updated_parameters["consumer_groups"]["size"],
            "producer_throughput": producer_throughput_byte,
            "records_per_sec": max(1, producer_throughput_byte // record_size_byte),
            "num_records_producer": num_records_producer,
            "num_records_consumer": num_records_consumer,
            "test_id": test_id,
            "throughput_series_id": throughput_series_id,
            "topic_name": topic_name,
            "depletion_topic_name": depletion_topic_name
        }

        updated_event = {
            "test_specification": test_specification,
            "current_test": {
                "index": updated_test_index,
                "parameters": updated_parameters
            }
        }

        return updated_event



def skip_remaining_throughput(index, event):
    parameters = [key for key in event["test_specification"]["parameters"].keys()]
    throughput_index = parameters.index("cluster_throughput_mb_per_sec")

    # set all parameters up to cluster_throughput_mb_per_sec to 0
    reset_index = {
        index_name:0 for i, index_name in enumerate(index) if i<=throughput_index
    }

    # keep values of other parameters and increase next parameter after cluster_throughput_mb_per_sec
    (updated_index, _) = increment_test_index({**index, **reset_index}, event, pos=throughput_index+1)

    # choose new series id, as we are starting with new throughput
    throughput_series_id = random.randint(0,100000)

    return (updated_index, throughput_series_id)



def update_parameters_for_depletion(event, context):
    test_specification = event["test_specification"]
    test_parameters = test_specification["parameters"]
    test_index = event["current_test"]["index"]
    test_id = event["current_test"]["parameters"]["test_id"]
    throughput_series_id = event["current_test"]["parameters"]["throughput_series_id"]
    depletion_duration_sec = test_specification["depletion_configuration"]["approximate_timeout_hours"] * 60 * 60

    # overwrite throughput of tests with default value for credit depletion
    depletion_parameters = {
        **test_parameters,
        "duration_sec": list(map(lambda _: depletion_duration_sec, test_parameters["duration_sec"])),
        "cluster_throughput_mb_per_sec": list(map(lambda _: -1, test_parameters["cluster_throughput_mb_per_sec"])),
    }

    # keep index unchanged and determine parameters for depletion
    return update_parameters({**test_specification, "parameters": depletion_parameters}, test_index, test_id, throughput_series_id)