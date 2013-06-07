package main.java.spouts;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class WordReader implements IRichSpout
{
	private SpoutOutputCollector collector;
	private FileReader fileReader;
	private boolean completed = false;
	private TopologyContext context;
	public boolean isDistributed() {return false;}
	
	public void ack(Object msgId){
		System.out.println("OK:"+msgId);
	}
	public void close(){}
	public void fail(Object msgId){
		System.out.println("FAIL:"+msgId);
	}
	@Override
	public void activate() {
		// TODO Auto-generated method stub	
	}
	@Override
	public void deactivate() {
		// TODO Auto-generated method stub	
	}
	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
	/* The method will emit each file line */
	
	public void nextTuple(){
		/* This method is called forever. If we have read the file we will
		 * wait and then return.
		 */
		if(completed){
			try{
				Thread.sleep(1);
			}catch(InterruptedException e){
				// Just Catch it. Do nothing
			}
			return;
		}
		String str;
		
		//Open the reader
		BufferedReader reader = new BufferedReader(fileReader);
		try{
			//Read all lines
			while((str = reader.readLine())!=null){
				/*
				 * Emit a new value with that line
				 */
				this.collector.emit(new Values(str),str);
			}
		}catch(Exception e){
			throw new RuntimeException("Error reading tuple",e);
		}finally{
			completed=true;
		}
	}
	/*
	 * Create the file and get the collector Object
	 */
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector){
		try{
			this.context=context;
			this.fileReader = new FileReader(conf.get("wordsFile").toString());
		}catch(FileNotFoundException e){
			throw new RuntimeException("Error reading file ["+conf.get("wordFile")+"[");
		}
		this.collector = collector;
	}
	
	/*
	 * Declare the output field "word"
	 */
	public void declareOutputFields(OutputFieldsDeclarer declarer){
		declarer.declare(new Fields("line"));
	}
}