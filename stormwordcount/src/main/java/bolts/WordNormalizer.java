package main.java.bolts;
/*
 * This bolt is responsible for takig each line , normalize it by splitting the line into words
 * convert all words to lowercase and trim them.
 */

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.tuple.Tuple;

public class WordNormalizer implements IRichBolt{
	private OutputCollector collector;

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector=collector;
		
	}

	/*
	 * receive lines from the words file and process to normalize this line
	 * put the words in lower case and split ine to get all words in this.
	 */
	@Override
	public void execute(Tuple input) {
		String sentence = input.getString(0);
		String[] words = sentence.split("");
		for(String word : words){
			word = word.trim();
			if(!word.isEmpty()){
				word = word.toLowerCase();
				//emit the word
				List a = new ArrayList();
				a.add(input);
				collector.emit(a,new Values(word));
			}
		}
		//acknowledge the tuple
		collector.ack(input);
	}

	@Override
	public void cleanup() {}

	/*
	 * This bolt will only emit the field word
	 */
	@Override
	public void declareOutputFields(
			OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
	
	
}