/*
 * This bolt takes in a query form the user in the form of a tuple of start and end nodes from the console
 * or read from a continuous stream of queries coming in from a spout (querySourceSpout.java)
 * as the need may be. 
 */

package storm.main.java.bolts;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.List;

import javax.ws.rs.core.MediaType;

import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.*;
import org.neo4j.graphdb.index.UniqueFactory;
import org.neo4j.graphalgo.impl.shortestpath.Dijkstra;
import org.neo4j.graphalgo.impl.util.DoubleAdder;
import org.neo4j.graphalgo.impl.util.DoubleComparator;
import org.neo4j.graphalgo.impl.util.DoubleEvaluator;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.graphdb.index.UniqueFactory;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.rest.graphdb.RestAPI;
import org.neo4j.rest.graphdb.RestAPIFacade;
import org.neo4j.rest.graphdb.RestGraphDatabase;
import org.neo4j.rest.graphdb.entity.RestNode;
import org.neo4j.rest.graphdb.query.RestCypherQueryEngine;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

@SuppressWarnings({ "serial", "unused", "rawtypes" })
public class queryServiceBolt implements IRichBolt
{
	//location of the database
	public static final String SERVER_ROOT_URI = "http://localhost:7474/db/data/";
	//location of the graph.dat file
	public static final String DATA_ROOT = "src/resources/query.dat";
	//name of the index to use
	public static final String NINDEX = "nindex";
	public static final String RINDEX = "rindex";
	RestNode temp1,temp2;
	RestAPI api2;
	RestCypherQueryEngine engine2;
	
	//counter operation handlers
	FileOutputStream fop1 =null;
	FileOutputStream fop2 =null,fop3 = null;
	File file1;
	File file2,file3;
	
	private static enum RelTypes implements RelationshipType
	{
		LINKS
	}
	
	@Override
	public void prepare(Map stormConf, TopologyContext context,OutputCollector collector) {
		//check if database is running or not
		checkDatabaseIsRunning();
		System.out.println("Starting Test . . ");
		api2 = new RestAPIFacade(SERVER_ROOT_URI);
		System.out.println("API Started . . ");
		engine2 = new RestCypherQueryEngine(api2);
		System.out.println("Engine Created . . ");
		//file
		try {
			file1 = new File("src/storm/main/Outputs/query-tuples-time-50bolts20spouts.dat");
			file2 = new File("src/storm/main/Outputs/query-tuples-memory.dat");
			file3 = new File("src/storm/main/Outputs/query-tuples-cpu.dat");
			fop1 =new FileOutputStream(file1);
			fop2 =new FileOutputStream(file2);
			fop3 =new FileOutputStream(file3);
			}catch(Exception e) {
				System.out.println("[EXCEPTION] : File could not be opened to write!");
			}
		}
	@Override
	public void cleanup(){
		try {
			fop1.close();
			fop2.close();
		    }
		catch(Exception e) {}
	}
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {}
	@Override
	public Map<String, Object> getComponentConfiguration() {return null;}
	@Override
	public void execute(Tuple input) {
		try {
			//Store the result to Finally Print at once
			StringBuilder result = new StringBuilder();

			//Get the tuple and split into fields
			String sentence = input.getString(0);
			String[] fields = sentence.split(" ");
			if(fields[0].equals("q")) {
				//code to find the shortest path using Dijkstra's in REST API
				String node1 = fields[1];
				String node2 = fields[2];
				//>>get node from property
				Map<String, Object> map1 = new HashMap<String, Object>();
				map1.put("name", node1);
				RestNode n1 = api2.getOrCreateNode(api2.index().forNodes("nindex", MapUtil.stringMap(IndexManager.PROVIDER, "lucene", "type", "fulltext")), "name", node1, map1);			
				Map<String, Object> map2 = new HashMap<String, Object>();
				map2.put("name", node2);
				RestNode n2 = api2.getOrCreateNode(api2.index().forNodes("nindex", MapUtil.stringMap(IndexManager.PROVIDER, "lucene", "type", "fulltext")), "name", node2, map2);
				
				//### ANALYTICS - TIME - MEMORY ###
				final long startTime = System.currentTimeMillis();
				long memoryUsed1 = bytesToMegabytes(Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory());
				//### ANALYTICS - TIME - MEMORY ###
				
				/*
				//------------------------------------------------------------------------
				//####################### Using REST API Calls ###########################
				//------------------------------------------------------------------------
				//>> Node 1
				String node1Uri = SERVER_ROOT_URI+"node/"+n1.getId();
				//>> Node 2
				String node2Uri = SERVER_ROOT_URI+"node/"+n2.getId();
				//>>Path Finding URI
				String pathUri = node1Uri+"/path";
				//>> Parameters in the jsonPayload
				String jsonPayload = "{\"to\":\""+node2Uri+"\","
						+ "\"cost_property\":\"cost\","
						+ "\"relationships\":"
						+ "{\"type\":\"LINKS\","
						+ "\"direction\":\"out\"},"
						+ "\"algorithm\":\"dijkstra\"}";
				//send the REST http request
				WebResource resource = Client.create().resource(pathUri);
				ClientResponse response = resource.accept(MediaType.APPLICATION_JSON)
						.type(MediaType.APPLICATION_JSON)
						.entity(jsonPayload)
						.post(ClientResponse.class);
				
				BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntityInputStream()));
				StringBuilder sb = new StringBuilder();
				String line;
				while((line=rd.readLine())!=null)
					sb.append(line+"\n");
				rd.close();
				
				//### ANALYTICS - TIME ###
				final long midTime = System.currentTimeMillis();
				
				//### ANALYTICS - TIME ###
				
				result.append("\n|================================= REST-API Query Result =================================|\n");
				result.append("|RESPONSE : "+sb.toString()+"\n");
				result.append("|QUERY TIME (apprx): "+(midTime-startTime)+" ms\n");
				result.append("|MEMORY (apprx) : "+bytesToMegabytes(Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory())+" Mb\n");
                result.append("|----------------------------------- JAVA Query Result -----------------------------------|\n");
                */
                
				//--------------------------------------------------------------------
				//################### Non-REST, GraphAlgoFactory #####################
				//--------------------------------------------------------------------
				
				final DoubleEvaluator eval = new DoubleEvaluator("cost");
		        final DoubleAdder adder = new DoubleAdder();
		        final DoubleComparator comparator = new DoubleComparator();
		        
				Dijkstra<Double> dijkstra = new Dijkstra<Double>(0.0d, n1,
                        n2, eval, adder, comparator, Direction.OUTGOING,
                        RelTypes.values());
				
				result.append("|PATH COSTS : " + dijkstra.getCost()+"\n|PATH : ");
				
				List<PropertyContainer> path = dijkstra.getPath();
		        StringBuilder builder = new StringBuilder();
		        int count=0;//for controlling the arrow printing
		        for(PropertyContainer cnt : path){
		        	if(count==0)
		        		count=1;
		        	else
		        		builder.append("-->");
		        
		        	if(cnt instanceof RestNode){
		        		RestNode node = (RestNode) cnt;
		                builder.append(node.getProperty("name"));
		        	}
		        	if (cnt instanceof Relationship) {
		                Relationship relt = (Relationship) cnt;
		                //builder.append(relt.getType());
		                builder.append('[');
		                builder.append(relt.getProperty("cost"));
		                builder.append(']');
		            	}
		            }
		            result.append(builder.toString()+"\n");
		            
		            
		            
		            //### ANALYTICS - TIME ###
					long queryTime = System.currentTimeMillis()-startTime;
					long memoryUsed2 = bytesToMegabytes(Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory());
					long memoryUsed = memoryUsed2-memoryUsed1;
					//### ANALYTICS - TIME ###
					
					//### ANALYTICS - QueryTupleCount ###
					//Get the tuple count, increment by one, and reset the tuple_count to new value
					Map<String, Object> counter = new HashMap<String, Object>();
					counter.put("name", "tuplecounter");
					RestNode n_count = api2.getOrCreateNode(api2.index().forNodes("nindex", MapUtil.stringMap(IndexManager.PROVIDER, "lucene", "type", "fulltext")), "name", "tuplecounter", counter);
					long tuple_count = Long.parseLong(n_count.getProperty("count").toString());
					final long startTimeUniversal = Long.parseLong(n_count.getProperty("InitialTime").toString());
					tuple_count=tuple_count+1;
					n_count.setProperty("count", tuple_count);
					
					/*
					//Write the tuple <queries,time> to the data file
					StringBuilder tups = new StringBuilder();
					tups.append(tuple_count);
					tups.append(",");
					tups.append(System.currentTimeMillis()-startTimeUniversal);
					//byte[] contentInBytes = tups.toString().getBytes();
					fop1.write(tups.toString().getBytes());
					fop1.write(System.getProperty("line.separator").getBytes());
					fop1.flush();
					*/
					
					//### ANALYTICS - CPU MEMORY ###
		            long memorySize=((com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean()).getTotalPhysicalMemorySize();
		    		//System.out.println("Total Memory : "+(memorySize/(1024*1024))+"Mb");
		    	    
		    		long freeMem = ((com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean()).getFreePhysicalMemorySize();
		    		//System.out.println("Free Memory : "+(freeMem/(1024*1024))+"Mb");
		    		
		    		double cpuLoad = ((com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean()).getSystemCpuLoad();
		    		//System.out.println("Total Memory : "+(cpuLoad*100)+"%");
		    		
		    		//Write the tuple <queries,memory> to the data file
					StringBuilder tups2 = new StringBuilder();
					tups2.append(tuple_count);
					tups2.append(",");
					tups2.append(((memorySize-freeMem)/(1024*1024)));
					byte[] contentInBytes2 = tups2.toString().getBytes();
					fop2.write(tups2.toString().getBytes());
					fop2.write(System.getProperty("line.separator").getBytes());
					fop2.flush();
					//write the tuples <queries,cpu> to data file
					StringBuilder tups3 = new StringBuilder();
					tups3.append(tuple_count);
					tups3.append(",");
					tups3.append((cpuLoad*100));
					byte[] contentInBytes3 = tups3.toString().getBytes();
					fop3.write(tups3.toString().getBytes());
					fop3.write(System.getProperty("line.separator").getBytes());
					fop3.flush();
					
		            //### ANALYTICS - CPU MEMORY ###
					
					result.append("|QUERY-TIME (apprx) : "+queryTime+" ms\n");
					result.append("|MEMORY (apprx) : "+memoryUsed+" Mb\n");
		            result.append("|=========================================================================================|\n");
					System.out.println(result.toString());
			}
			//#####################################################################
		}catch(Exception e) {
			StringBuffer err = new StringBuffer();
			err.append("\n|================================= REST-API Query Result =================================|\n");
			err.append("|[QUERY-ERROR] : No Direct Route Found from : "+temp1+"->"+temp2+"\n");
			err.append("|=========================================================================================|\n");
		}
	}
	private long bytesToMegabytes(long l) {
		long mb = l/(1024*1024);
		return mb;
	}
	public static void checkDatabaseIsRunning() {
		WebResource resource = Client.create().resource(SERVER_ROOT_URI);
		ClientResponse response = resource.get(ClientResponse.class);
		System.out.println(String.format("Server Running Successfully\nStatus Code : [%s]"
				+ "\nLocation : [%s]",response.getStatus(),SERVER_ROOT_URI));
	}
	
}
