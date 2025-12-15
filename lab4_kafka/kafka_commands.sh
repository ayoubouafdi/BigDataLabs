docker start hadoop-master hadoop-slave1 hadoop-slave2
docker exec -it hadoop-master bash


./start-hadoop.sh
./start-kafka-zookeeper.sh



kafka-topics.sh --create --bootstrap-server localhost:9092 \ --replication-factor 1 --partitions 1 \ --topic Hello-Kafka

kafka-topics.sh --list --bootstrap-server localhost:9092




kafka-console-producer.sh --bootstrap-server localhost:9092 --topic Hello-Kafka

kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic Hello-Kafka --from-beginning