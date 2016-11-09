# Storm-Work
Bolts and Topologies for Storm processing

Create topic
./kafka-topics.sh --zookeeper localhost:2181 --create --partitions 1 --topic addresses --replication-factor 1

Run with
$ storm jar storm-streaming-1.0-SNAPSHOT.jar com.oliver.streaming.impl.topologies.KafkaPhoenixTopology /etc/storm_demo/config.properties

Producer sample:
$ ./kafka-console-producer.sh --broker-list localhost:6667 --topic addresses

Consumer sample:
$ ./kafka-console-consumer.sh --zookeeper localhost:2181 --topic addresses

Updated:
$ storm jar storm-streaming-25-1.0-SNAPSHOT.jar com.oliver.streaming.impl.topologies.PhoenixTest /etc/storm/conf/config.yaml
