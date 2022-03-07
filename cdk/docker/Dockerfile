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

FROM alpine:3.15

ENV KAFKA_VERSION=2.8.0
ENV SCALA_VERSION=2.12

RUN apk add --no-cache tini zsh bash parallel mawk procps wget jq py-pip openjdk17-jre-headless \
  && pip install awscli \
  && wget -qO - https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz | tar xz -C /opt/ \
  && wget --no-verbose https://github.com/aws/aws-msk-iam-auth/releases/download/v1.1.1/aws-msk-iam-auth-1.1.1-all.jar -P /opt/kafka_${SCALA_VERSION}-${KAFKA_VERSION}/libs/

RUN cp /usr/lib/jvm/java-*/lib/security/cacerts /opt/kafka.client.truststore.jks

COPY run-kafka-command.sh /opt/
COPY client-*.properties /opt/

ENTRYPOINT [ "tini", "--",  "/opt/run-kafka-command.sh" ]