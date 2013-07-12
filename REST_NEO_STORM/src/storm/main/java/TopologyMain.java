package storm.main.java;

import storm.main.java.spouts.graphQuerySpout;
import storm.main.java.spouts.graphUpdateSpout;
import storm.main.java.bolts.graphUpdateBolt;
import storm.main.java.bolts.queryServiceBolt;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

@SuppressWarnings("unused")
public class TopologyMain {
	public static void main(String[] args) throws InterruptedException {
		/* >>>>   TOPOLOGY DEFINITION <<<< */
		TopologyBuilder builder = new TopologyBuilder();
		//builder.setSpout("give-update", new graphUpdateSpout());
		builder.setSpout("give-query", new graphQuerySpout(),20)
			.setNumTasks(1);
		/*builder.setBolt("do-update", new graphUpdateBolt())
			.setNumTasks(1)
			.shuffleGrouping("give-update");*/
		builder.setBolt("query-service", new queryServiceBolt(),10)//number of executors
			.setNumTasks(1)//number of tasks per component
			.shuffleGrouping("give-query");
		
		/* >>>> CONFIGURATION  <<<< */
		Config conf = new Config();
		conf.put("updates", "src/storm/main/Resources/update.dat");
		conf.put("query", "src/storm/main/Resources/query.dat");
		conf.setDebug(false);
		
		/* >>>> TOPOLOGY RUN <<<< */
		conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING,1);
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("Dijkstra-storm-neo4j", conf, builder.createTopology());
		
		/* >>>> INITIALIZE COUNTERS <<<<*/
		try{Initialize_query_count.main();}
		catch(Exception e){e.printStackTrace();}
		
		/* Sleep while the Topology Runs on another thread
		 * When the timer runs out, the control is back with this thread,
		 * and the shutdown signal for the cluster is given.
		 */		
		Thread.sleep(1000000000);
		cluster.shutdown();
	}
}