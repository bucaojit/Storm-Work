package com.oliver.streaming.impl.topologies;

import org.apache.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;

import com.oliver.streaming.impl.bolts.RouteBolt;
import com.oliver.streaming.impl.bolts.SubmitBolt;

public class KafkaPhoenixTopology extends BaseKafkaPhoenixTopology{
	private static final Logger LOG = Logger.getLogger(KafkaPhoenixTopology.class);
	
	private static String ROUTE_BOLT = "ROUTE_BOLT";
	
	//String configLocation;
	public KafkaPhoenixTopology(String configLocation) throws Exception{
		
		//this.configLocation = configLocation;
		super(configLocation);
	}
	
    public static void main(String[] args) throws Exception {
        String configFileLocation = args[0];


        KafkaPhoenixTopology kpTopology = new KafkaPhoenixTopology(configFileLocation);
        kpTopology.buildAndSubmit();
    }
    
    public void buildAndSubmit() throws Exception {
    	 TopologyBuilder builder = new TopologyBuilder();
    	 Config config = new Config();
         config.setDebug(true);
         String nimbusHost = topologyConfig.getProperty("nimbus.host");
         config.put(Config.NIMBUS_HOST, nimbusHost);
         
         configureKafkaSpout(builder);
         configureRouteBolt(builder);
         
         builder.setBolt("submitter", new SubmitBolt())
            .shuffleGrouping(ROUTE_BOLT);
         
         try {
             StormSubmitter.submitTopology("simple-topology", config, builder.createTopology());
         } catch (Exception e) {
             LOG.error("Error submiting Topology", e);
         }

    }
    
    public int configureKafkaSpout(TopologyBuilder builder) {
        KafkaSpout kafkaSpout = constructKafkaSpout();

        int spoutCount = Integer.valueOf(topologyConfig.getProperty("spout.thread.count"));
        int boltCount = Integer.valueOf(topologyConfig.getProperty("bolt.thread.count"));

        builder.setSpout("kafkaSpout", kafkaSpout, spoutCount);
        return boltCount;
    }
    
    private KafkaSpout constructKafkaSpout() {
        KafkaSpout kafkaSpout = new KafkaSpout(constructKafkaSpoutConf());
        return kafkaSpout;
    }

    private SpoutConfig constructKafkaSpoutConf() {
        BrokerHosts hosts = new ZkHosts(topologyConfig.getProperty("kafka.zookeeper.host.port"));
        String topic = topologyConfig.getProperty("kafka.topic");
        String zkRoot = topologyConfig.getProperty("kafka.zkRoot");
        String consumerGroupId = topologyConfig.getProperty("kafka.consumer.group.id");

        SpoutConfig spoutConfig = new SpoutConfig(hosts, topic, zkRoot, consumerGroupId);
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

        return spoutConfig;
    }
    
    public void configureRouteBolt(TopologyBuilder builder) {
        RouteBolt routeBolt = new RouteBolt(true);
        //Defines new bolt in topology
        builder.setBolt(ROUTE_BOLT, routeBolt, 2).shuffleGrouping("kafkaSpout");
    }



}
