docker run -d \
  --net=host \
  --name=zoo \
  -e ZOOKEEPER_CLIENT_PORT=2181 \
  -e ZOOKEEPER_TICK_TIME=2000 \
  -e ZOOKEEPER_SYNC_LIMIT=2 \
  confluentinc/cp-zookeeper:7.0.0
