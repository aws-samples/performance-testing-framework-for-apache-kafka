#!/bin/zsh -x

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


set -eo pipefail
setopt shwordsplit

signal_handler() {
    echo "trap triggered by signal $1"
    ps -weo pid,%cpu,%mem,size,vsize,cmd --sort=-%mem
    trap - EXIT
    exit $2
}

output_mem_usage() {
    while true; do
        ps -weo pid,%cpu,%mem,rss,cmd --sort=-%mem
        sleep 60
    done
}

wait_for_all_tasks() {
    # wait until all jobs/containers are actually running
    echo -n "waiting for jobs: "
    while [[ ${running_jobs:--1} -lt "$args[--num-jobs]" ]]; do
        sleep 1
        echo -n "."
        running_jobs=$(aws --region $args[--region] --output text batch list-jobs --array-job-id ${AWS_BATCH_JOB_ID%:*} --job-status RUNNING --query 'jobSummaryList | length(@)') || running_jobs=-1
    done
    echo " done"
}

producer_command() {
    KAFKA_HEAP_OPTS="-Xms1G -Xmx3584m" /opt/kafka_?.??-?.?.?/bin/kafka-producer-perf-test.sh \
        --topic $TOPIC \
        --num-records $(printf '%.0f' $args[--num-records-producer]) \
        --throughput $THROUGHPUT \
        --record-size $(printf '%.0f' $args[--record-size-byte]) \
        --producer-props bootstrap.servers=$PRODUCER_BOOTSTRAP_SERVERS ${(@s/ /)args[--producer-props]} \
        --producer.config /opt/client.properties 2>&1
}

consumer_command() {
    for config in $args[--consumer-props]; do
        if [[ $config ==  *SSL* ]]; then    # skip encryption config as it's already part of the properties file
            continue
        fi

        echo $config >> /opt/client.properties
    done

    cat /opt/client.properties

    KAFKA_HEAP_OPTS="-Xms1G -Xmx3584m" /opt/kafka_?.??-?.?.?/bin/kafka-consumer-perf-test.sh \
        --topic $TOPIC \
        --messages $(printf '%.0f' $args[--num-records-consumer]) \
        --broker-list $CONSUMER_BOOTSTRAP_SERVERS \
        --consumer.config /opt/client.properties \
        --group $CONSUMER_GROUP \
        --print-metrics \
        --show-detailed-stats \
        --timeout 16000 2>&1
}

query_msk_endpoint() {
    if [[ $args[--bootstrap-broker] = "-" ]]
    then
        case $args[--producer-props] in
            *SASL_SSL*)
                PRODUCER_BOOTSTRAP_SERVERS=$(aws --region $args[--region] kafka get-bootstrap-brokers --cluster-arn $args[--msk-cluster-arn]  --query "BootstrapBrokerStringSaslIam" --output text)
                ;;
            *SSL*)
                PRODUCER_BOOTSTRAP_SERVERS=$(aws --region $args[--region] kafka get-bootstrap-brokers --cluster-arn $args[--msk-cluster-arn]  --query "BootstrapBrokerStringTls" --output text)
                ;;
            *)
                PRODUCER_BOOTSTRAP_SERVERS=$(aws --region $args[--region] kafka get-bootstrap-brokers --cluster-arn $args[--msk-cluster-arn]  --query "BootstrapBrokerString" --output text)
                ;;
        esac

        case $args[--consumer-props] in
            *SASL_SSL*)
                CONSUMER_BOOTSTRAP_SERVERS=$(aws --region $args[--region] kafka get-bootstrap-brokers --cluster-arn $args[--msk-cluster-arn]  --query "BootstrapBrokerStringSaslIam" --output text)
                ;;
            *SSL*)
                CONSUMER_BOOTSTRAP_SERVERS=$(aws --region $args[--region] kafka get-bootstrap-brokers --cluster-arn $args[--msk-cluster-arn]  --query "BootstrapBrokerStringTls" --output text)
                ;;
            *)
                CONSUMER_BOOTSTRAP_SERVERS=$(aws --region $args[--region] kafka get-bootstrap-brokers --cluster-arn $args[--msk-cluster-arn]  --query "BootstrapBrokerString" --output text)
                ;;
        esac
    else
        PRODUCER_BOOTSTRAP_SERVERS=$args[--bootstrap-broker]
        CONSUMER_BOOTSTRAP_SERVERS=$args[--bootstrap-broker]
    fi
}


trap 'signal_handler SIGTERM 15 $LINENO' SIGTERM
# trap 'signal_handler EXIT $? $LINENO' EXIT


# parse aruments
zparseopts -A args -E -- -region: -msk-cluster-arn: -bootstrap-broker: -num-jobs: -command: -replication-factor: -topic-name: -depletion-topic-name: -record-size-byte: -records-per-sec: -num-partitions: -duration-sec: -producer-props: -num-producers: -num-records-producer: -consumer-props: -size-consumer-group: -num-records-consumer:

date
echo "parsed args: ${(kv)args}"
echo "running command: $args[--command]"

if [[ -f "$ECS_CONTAINER_METADATA_FILE" ]]; then
    CLUSTER=$(cat $ECS_CONTAINER_METADATA_FILE | jq -r '.Cluster')
    INSTANCE_ARN=$(cat $ECS_CONTAINER_METADATA_FILE | jq -r '.ContainerInstanceARN')
    echo -n "running on instance: "
    aws ecs describe-container-instances --region $args[--region] --cluster $CLUSTER --container-instances $INSTANCE_ARN | jq -r '.containerInstances[].ec2InstanceId'
fi


# periodically output mem usage for debugging purposes
# output_mem_usage &


# create authentication config properties
case $args[--producer-props] in
    *SASL_SSL*)
        cp /opt/client-iam.properties /opt/client.properties
        ;;
    *SSL*)
        cp /opt/client-tls.properties /opt/client.properties
        ;;
    *)
        touch /opt/client.properties
        ;;
esac


# obtain cluster endpoint; retry if the command fails, eg, when credentials cannot be obtained from environment
query_msk_endpoint
while [ -z "$PRODUCER_BOOTSTRAP_SERVERS"  ]; do
    sleep 10
    query_msk_endpoint
done


case $args[--command] in
create-topics)
    # create topics with 5 sec retention for depletion task

    while
        ! KAFKA_HEAP_OPTS="-Xms128m -Xmx512m" /opt/kafka_?.??-?.?.?/bin/kafka-topics.sh \
            --bootstrap-server $PRODUCER_BOOTSTRAP_SERVERS \
            --command-config /opt/client.properties \
            --list \
        | grep -q "$args[--depletion-topic-name]"
    do
        # try to create topic if none exists
        KAFKA_HEAP_OPTS="-Xms128m -Xmx512m" \
            /opt/kafka_?.??-?.?.?/bin/kafka-topics.sh \
            --bootstrap-server $PRODUCER_BOOTSTRAP_SERVERS \
            --create \
            --topic "$args[--depletion-topic-name]" \
            --partitions $args[--num-partitions] \
            --replication-factor $args[--replication-factor] \
            --config retention.ms=5000 \
            --command-config /opt/client.properties

        sleep 5
    done

    while
        # check if topic already exists
        ! KAFKA_HEAP_OPTS="-Xms128m -Xmx512m" /opt/kafka_?.??-?.?.?/bin/kafka-topics.sh \
            --bootstrap-server $PRODUCER_BOOTSTRAP_SERVERS \
            --command-config /opt/client.properties \
            --list \
        | grep -q "$args[--topic-name]"
    do
        # try to create topics for actual performance tests
        KAFKA_HEAP_OPTS="-Xms128m -Xmx512m" \
            /opt/kafka_?.??-?.?.?/bin/kafka-topics.sh \
            --bootstrap-server $PRODUCER_BOOTSTRAP_SERVERS \
            --create \
            --topic "$args[--topic-name]" \
            --partitions $args[--num-partitions] \
            --replication-factor $args[--replication-factor] \
            --command-config /opt/client.properties

        sleep 5
    done

    # list the created topics for debugging purposes
    KAFKA_HEAP_OPTS="-Xms128m -Xmx512m" /opt/kafka_?.??-?.?.?/bin/kafka-topics.sh \
        --bootstrap-server $PRODUCER_BOOTSTRAP_SERVERS \
        --command-config /opt/client.properties \
        --list
    ;;

delete-topics)
    # delete all topics on the cluster (except for consumer group related ones)
    KAFKA_HEAP_OPTS="-Xms128m -Xmx512m" /opt/kafka_?.??-?.?.?/bin/kafka-topics.sh \
        --bootstrap-server $PRODUCER_BOOTSTRAP_SERVERS \
        --command-config /opt/client.properties \
        --delete \
        --topic 'test-id-\d+--throughput-series-id-\d+((--\S+-\d+)+|--depletion)' \
        || :    # don't fail if no topic exists
    ;;

deplete-credits)
    TOPIC="$args[--depletion-topic-name]"
    THROUGHPUT=-1

    wait_for_all_tasks

    if [[ "$AWS_BATCH_JOB_ARRAY_INDEX" -lt $args[--num-producers] ]]; then
        producer_command | mawk -W interactive '
                 /TimeoutException/ { if(++exceptions % 100000 == 0) {print "Filtered", exceptions, "TimeoutExceptions."} } 
                !/TimeoutException/ { print $0 }
            '
    else
        CONSUMER_GROUP="$(((AWS_BATCH_JOB_ARRAY_INDEX - args[--num-producers]) / args[--size-consumer-group]))"

        consumer_command
    fi
    ;;
    
run-performance-test)
    TOPIC="$args[--topic-name]"
    THROUGHPUT=$(printf '%.0f' $args[--records-per-sec])

    wait_for_all_tasks

    # build the command for the actual performance test, the array index determines whether to produce or consume with this job
    if [[ "$AWS_BATCH_JOB_ARRAY_INDEX" -lt $args[--num-producers] ]]; then
        producer_command | mawk -W interactive '
                /TimeoutException/ { if(++timeouts % 100000 == 0) {print "Filtered", timeouts, "TimeoutExceptions."} }
                /error/            { errors++ }
                !/TimeoutException/ { print $0 }
                END {
                    printf "{ \"type\": \"producer\", \"test_summary\": \"%s\", \"timeout_exception_count\": \"%d\", \"error_count\": \"%d\" }\n", $0, timeouts, errors
                }
            '
    else
        CONSUMER_GROUP="$(((AWS_BATCH_JOB_ARRAY_INDEX - args[--num-producers]) / args[--size-consumer-group]))"

        consumer_command | mawk -W interactive '
                /WARNING: Exiting before consuming the expected number of messages/ { warnings++ }
                // { print $0 }
                END {
                    printf  "{ \"type\": \"consumer\", \"too_few_messages_count\": \"%d\" }\n", warnings
                }
            '
    fi
    ;;

*)
    print NIL
    ;;
esac