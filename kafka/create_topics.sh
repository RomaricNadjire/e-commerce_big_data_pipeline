docker exec training_kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --create \
  --topic transactions \
  --partitions 3 \
  --replication-factor 1 \
  --bootstrap-server kafka:9092