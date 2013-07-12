package storm.main.java.spouts;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

@SuppressWarnings({ "serial", "unused", "rawtypes" })
public class graphUpdateSpout implements IRichSpout
{
	private SpoutOutputCollector collector;
	private FileReader fileReader;
	private boolean completed = false;
	private TopologyContext context;
	public boolean isDistributed() {return false;}
	@Override
	public void close() {}
	@Override
	public void ack(Object msgId) {/*System.out.println("OK:"+msgId);*/}
	@Override
	public void fail(Object msgId) {/*System.out.println("FAIL:"+msgId);*/}
	@Override
	public void activate() {}
	@Override
	public void deactivate() {}
	@Override
	public Map<String, Object> getComponentConfiguration() {return null;}
	@Override
	
	public void nextTuple() {
		/* This File is called forever.If we have read the file we will wait and then return */
		if(completed){
			try{Thread.sleep(1);}catch(InterruptedException e){/* Just Catch it. Do nothing */}
			return;
		}
		String str;
		BufferedReader reader = new BufferedReader(fileReader);//Open the reader
		try{
			//Read all lines and Emit a new value with that line
			while((str = reader.readLine())!=null)
			{
				this.collector.emit(new Values(str),str);
			}
		}catch(Exception e){
			throw new RuntimeException("Error reading tuple",e);
		}finally{
			completed=true;
		}
	}
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("line"));
		
	}
	
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		try{
			this.context=context;
			this.fileReader = new FileReader(conf.get("updates").toString());
			}
		catch(FileNotFoundException e){
			throw new RuntimeException("Error reading file!");
		}
		this.collector = collector;
		
	}
	
	
	
}