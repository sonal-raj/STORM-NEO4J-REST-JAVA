package storm.main.java.bolts;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.*;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.graphdb.index.UniqueFactory;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.rest.graphdb.RestAPI;
import org.neo4j.rest.graphdb.RestAPIFacade;
import org.neo4j.rest.graphdb.RestGraphDatabase;
import org.neo4j.rest.graphdb.entity.RestNode;
import org.neo4j.rest.graphdb.query.RestCypherQueryEngine;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.tuple.Tuple;


@SuppressWarnings({ "serial", "unused", "rawtypes" })
public class graphUpdateBolt implements IRichBolt
{
	//location of the database
	public static final String SERVER_ROOT_URI = "http://localhost:7474/db/data/";
		
	//location of the update.dat file as the source of the update data
	public static final String DATA_ROOT = "src/resources/graph.dat";
	//name of the index to use
	public static final String NINDEX = "nindex";
	public static final String RINDEX = "rindex";
    RestAPI api;
    RestCypherQueryEngine engine;
	private static enum RelTypes implements RelationshipType
	{
		LINKS
	}
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) 
	{
		//check if database is running or not
		checkDatabaseIsRunning();
		//Initiating Services
		System.out.println("[STATUS] : Starting Test . . ");
		api = new RestAPIFacade(SERVER_ROOT_URI);
		System.out.println("[STATUS] : API Started . . ");
        engine = new RestCypherQueryEngine(api);
        System.out.println("[STATUS] : Engine Created . . ");
		//GraphDatabaseService graphDataService = new RestGraphDatabase(SERVER_ROOT_URI);
	}
	@Override
	public void cleanup() 
	{
	}
	public static void checkDatabaseIsRunning() {
		WebResource resource = Client.create().resource(SERVER_ROOT_URI);
		ClientResponse response = resource.get(ClientResponse.class);
		System.out.println(String.format("[STATUS] : Server Running Successfully\nStatus Code : [%s]"
				+ "\nLocation : [%s]",response.getStatus(),SERVER_ROOT_URI));
	}
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {/*declarer.declare(new Fields("word"));*/}
	@Override
	public Map<String, Object> getComponentConfiguration() {return null;}
	@Override
	public void execute(Tuple input)
	{   
		try
		{	//### ANALYTICS - TIME ###
			final long startTime = System.currentTimeMillis();
			//### ANALYTICS - TIME ###
			
			//Get the tuple and add to the database
			String sentence=input.getString(0);
			String fields[] = sentence.split(" ");
			if(fields[0].equals("a")){/*add an edge to the graph*/}
			if(fields[0].equals("r")){/*remove an edge*/}
			if(fields[0].equals("c")){/*change an edge property in real-time*/
				String node1 = fields[1];
				String node2 = fields[2];
				String relval = fields[3];
				
				//These are not needed while creating the database but while updating
				//get node 1
				Map<String, Object> map1 = new HashMap<String, Object>();
				map1.put("name", node1);
				RestNode n1 = api.getOrCreateNode(api.index().forNodes("nindex", MapUtil.stringMap(IndexManager.PROVIDER, "lucene", "type", "fulltext")), "name", node1, map1);
				//System.out.println("REST NODE ID is : "+n1.getId()+" REST URI :"+n1.getUri());
				
				//get node 2
				Map<String, Object> map2 = new HashMap<String, Object>();
				map2.put("name", node2);
				RestNode n2 = api.getOrCreateNode(api.index().forNodes("nindex", MapUtil.stringMap(IndexManager.PROVIDER, "lucene", "type", "fulltext")), "name", node2, map2);
				//System.out.println("REST NODE ID is : "+n2.getId()+" REST URI :"+n2.getUri());
				
				for(Relationship rel:n1.getRelationships(RelTypes.LINKS,Direction.OUTGOING))
				{
					if((rel.getEndNode().getId()==n2.getId())&&(rel.getStartNode().getId()==n1.getId()))
					{	
					//update the property to the new property
					rel.removeProperty("cost");
					rel.setProperty("cost", relval);
					//### ANALYTICS - TIME ###
					final long updateTime = System.currentTimeMillis()-startTime;
					System.out.println("[UPDATE] : Relation : "+n1.getId()+"->"+n2.getId()+" | Property(COST) to : "+relval+" | UpdateTime : "+updateTime+" ms");
					}	
				}
			}
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		
	}
}