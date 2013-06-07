package main.java.bolts;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class WordCounter implements IRichBolt {
	
	Integer id;
	String name;
	Map<String, Integer> counters;
	private OutputCollector collector;
	
	/*
	 * At the end of the spout, when the cluster is shutdown 
	 * We will show the word counters. 
	 */
	@Override
	public void cleanup() {
		System.out.println("-- Word Counter ["+name+"-"+id+"] --");
		for(Map.Entry<String, Integer> entry : counters.entrySet()){
			System.out.println(entry.getKey()+": "+entry.getValue());
		}
		
	}
	/* on each word we will count */
	@Override
	public void execute(Tuple input) {
		String str=input.getString(0);
		/* If the word does not exist in the map
		 * we will create one ,otherwise we will add 1 to the existing
		 */
		if(!counters.containsKey(str)){
			counters.put(str, 1);
		}else{
			Integer c = counters.get(str)+1;
			counters.put(str, c);
		}
		//Set the tuple as acknowledge
		collector.ack(input);		
	}
	
	/* On create */
	
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.counters = new HashMap<String, Integer>();
		this.collector = collector;
		this.name = context.getThisComponentId();
		this.id = context.getThisTaskId();
		
	}

	@Override
	public void declareOutputFields(
			OutputFieldsDeclarer paramOutputFieldsDeclarer) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
	
}