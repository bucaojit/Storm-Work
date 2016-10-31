package com.oliver.streaming.impl.bolts;

import java.util.Map;

import org.apache.log4j.Logger;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class RouteBolt extends BaseRichBolt{
	private static final Logger LOG = Logger.getLogger(RouteBolt.class);
	boolean persistAllEvents;
    private OutputCollector outputCollector;

    public RouteBolt(Boolean persistAllEvents) {
        this.persistAllEvents = persistAllEvents;
    }
	@Override
	public void execute(Tuple input) {
		LOG.info("About to process tuple[" + input + "]");
		
	      String sentence = input.getString(0);
	      String[] words = sentence.split(" ");
	      
	      for(String word: words) {
	         word = word.trim();
	         
	         if(!word.isEmpty()) {
	            word = word.toLowerCase();
	            outputCollector.emit(new Values(word));
	         }
	         
	      }
	      
	      outputCollector.ack(input);
        
	}
	@Override
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector outputCollector) {
        LOG.info("The PersistAllEvents Flag is set to: " + persistAllEvents);
        this.outputCollector = outputCollector;
		
	}
	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		arg0.declare(new Fields("word"));
	}

}
