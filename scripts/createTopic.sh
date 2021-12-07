#!/bin/bash

docker run -it --name list-topics --rm \
  --network=host \
  confluentinc/cp-kafka:7.0.0 kafka-topics \
 --bootstrap-server localhost:9092 \
 --create \
 --partitions 1 \
 --replication-factor 1 \
 --topic $1
