package main.java;

import main.java.spouts.WordReader;
import main.java.bolts.WordCounter;
import main.java.bolts.WordNormalizer;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class TopologyMain {
	public static void main(String[] args) throws InterruptedException {
		
		/* >>>>   TOPOLOGY DEFINITION <<<< */
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("word-reader", new WordReader());
		builder.setBolt("word-normalizer", new WordNormalizer()).shuffleGrouping("word-reader");
		builder.setBolt("word-counter", new WordCounter(),2).fieldsGrouping("word-normalizer", new Fields("word"));
		
		/* >>>> CONFIGURATION  <<<< */
		Config conf = new Config();
		conf.put("wordsFile", "src/main/resources/words.txt");
		conf.setDebug(true);
		
		/* >>>> TOPOLOGY RUN <<<< */
		conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING,1);
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("Storm-Word-Count", conf, builder.createTopology());
		/* Sleep while the Topology Runs on another thread
		 * When the timer runs out, the control is back with this thread,
		 * and the shutdown signal for the cluster is given.
		 */
		Thread.sleep(100000);
		cluster.shutdown();
	}
}