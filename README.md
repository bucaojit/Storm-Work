# Storm-Work
Bolts and Topologies for Storm processing

Run with
$ storm jar storm-streaming-1.0-SNAPSHOT.jar com.oliver.streaming.impl.topologies.KafkaPhoenixTopology /etc/storm_demo/config.properties

Producer sample:
$ ./kafka-console-producer --broker-list localhost:6667 --topic addresses

Consumer sample:
$ ./kafka-console-consumer --zookeeper localhost:2181 --topic addresses
