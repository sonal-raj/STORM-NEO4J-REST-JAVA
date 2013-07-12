package storm.main.java;

import java.io.*;
//import java.net.URI;
import java.net.URISyntaxException;
//import javax.ws.rs.core.MediaType;
//import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
//import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
//import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.rest.graphdb.RestGraphDatabase;
//import org.neo4j.rest.graphdb.GraphDatabaseFactory;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
//import org.neo4j.cypher.javacompat.*;
import org.neo4j.rest.graphdb.RestAPI;
import org.neo4j.rest.graphdb.entity.RestNode;
import org.neo4j.rest.graphdb.entity.RestRelationship;
import org.neo4j.rest.graphdb.index.RestIndex;
import org.neo4j.rest.graphdb.query.RestCypherQueryEngine;
import org.neo4j.rest.graphdb.RestAPIFacade;
//import org.neo4j.rest.graphdb.RestGraphDatabase;
//import org.neo4j.rest.graphdb.CypherRestShell;
//import org.neo4j.rest.graphdb.util.QueryResult;
//import static org.neo4j.helpers.collection.MapUtil.map;
import java.util.HashMap;
import java.util.Map;

public class Jbind_createDb{
	//location of the database
	public static final String SERVER_ROOT_URI = "http://localhost:7474/db/data/";
	//location of the graph.dat file
	public static final String DATA_ROOT = "src/storm/main/Resources/graph.dat";
	//name of the index to use
	public static final String NINDEX = "nindex";
	public static final String RINDEX = "rindex";
	//static BufferedWriter bw;
	
	@SuppressWarnings("unused")
	public static void main(String[] args) throws URISyntaxException {
		//check if database is running or not
		checkDatabaseIsRunning();
		
		System.out.println("Starting Test . . ");
		final RestAPI api = new RestAPIFacade(SERVER_ROOT_URI);
		System.out.println("API Started . . ");
        final RestCypherQueryEngine engine = new RestCypherQueryEngine(api);
        System.out.println("Engine Created . . ");
		GraphDatabaseService graphDataService = new RestGraphDatabase(SERVER_ROOT_URI);
		
		//###ANALYTICS-TIME###
		long file_index=2;
		long node_count=0;
		long startTime = System.currentTimeMillis();
		FileOutputStream fop =null;
		File file;
		try {
			file = new File("src/storm/main/Outputs/create-nodes-time-server.dat");
			fop =new FileOutputStream(file);
			}catch(Exception e) {
				System.out.println("Exception Occurred!!!");
			}
		//###ANALYTICS-TIME###
		
		//###ANALYTICS-QueryTupleCounts###
		/*
		 * create a node in Neo4j graphDb with the property called "tuple_count" and use
		 * it to store the number of tuples being processed. Each time a tuple has been processed,
		 * it's count is incremented by the bolt processing that tuple. 
		 */
		Map<String, Object> counter = new HashMap<String, Object>();
		counter.put("name", "tuplecounter");
		RestNode n_count = api.getOrCreateNode(api.index().forNodes("nindex", MapUtil.stringMap(IndexManager.PROVIDER, "lucene", "type", "fulltext")), "name", "tuplecounter", counter);
		n_count.setProperty("count", 0);
		System.out.println("NODE 1 : REST ID is : "+n_count.getId()+"|  REST URI :"+n_count.getUri());
		
		//###ANALYTICS-QueryTupleCounts###
		
		//read from the data file for nodes and relationship data
		try(BufferedReader br = new BufferedReader(new FileReader(DATA_ROOT)))
		{
			//get the tuples to add nodes to the database
			String tuple;
			
			while((tuple=br.readLine())!=null) {
				String[] fields = tuple.split(" ");
				if(fields[0].equals("a")) {
					String node1 = fields[1];
					String node2 = fields[2];
					String relval = fields[3];
					//These are not needed while creating the database but while updating
					
					//get node 1
					Map<String, Object> map1 = new HashMap<String, Object>();
					map1.put("name", node1);
					RestNode n1 = api.getOrCreateNode(api.index().forNodes("nindex", MapUtil.stringMap(IndexManager.PROVIDER, "lucene", "type", "fulltext")), "name", node1, map1);
					System.out.println("NODE 1 : REST ID is : "+n1.getId()+"|  REST URI :"+n1.getUri());
					
					//get node 2
					Map<String, Object> map2 = new HashMap<String, Object>();
					map2.put("name", node2);
					RestNode n2 = api.getOrCreateNode(api.index().forNodes("nindex", MapUtil.stringMap(IndexManager.PROVIDER, "lucene", "type", "fulltext")), "name", node2, map2);
					System.out.println("NODE 2 : REST ID is : "+n2.getId()+"|  REST URI :"+n2.getUri());
					
					//get relationship
					Map<String, Object> map3 = new HashMap<String, Object>();
					map3.put("cost", relval);
					String val = new String(node1+"to"+node2);
					@SuppressWarnings("unchecked")
					RestIndex<Relationship> indexx = (RestIndex<Relationship>) api.index().forRelationships("rindex", MapUtil.stringMap(IndexManager.PROVIDER, "lucene", "type", "fulltext"));
					RestRelationship rell = api.getOrCreateRelationship(indexx, "name", val, n1, n2, "LINKS", map3);
					System.out.println("REST Relation : "+rell.getId()+"|  REST RelURI :"+rell.getUri()+"\n");
				    
					//###ANALYTICS###
					//create the tuple of <nodes,time> and write to files
					node_count++;
					StringBuilder tups = new StringBuilder();
					tups.append(node_count);
					tups.append(",");
					tups.append((System.currentTimeMillis()-startTime));
					/*
					 * Write Code to Write to File "create-nodes-time-server.dat"
					 */
					byte[] contentInBytes = tups.toString().getBytes();
					fop.write(contentInBytes);
					fop.write(System.getProperty("line.separator").getBytes());
					fop.flush();
					//###ANALYTICS###
				}
			}
			//writer.close();
			fop.close();
			
		}catch(Exception e) {
			System.out.println("ERROR!!! Exception : ");
			e.printStackTrace();
		}
	}
	
	public static void checkDatabaseIsRunning() {
		WebResource resource = Client.create().resource(SERVER_ROOT_URI);
		ClientResponse response = resource.get(ClientResponse.class);
		System.out.println(String.format("Server Running Successfully\nStatus Code : [%s]"
				+ "\nLocation : [%s]",response.getStatus(),SERVER_ROOT_URI));
	}
}