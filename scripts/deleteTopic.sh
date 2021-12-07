#!/bin/bash

docker run -it --name delete-topic --rm \
  --network=host \
  confluentinc/cp-kafka:7.0.0 kafka-topics \
 --bootstrap-server localhost:9092 \
 --delete \
 --topic $1
