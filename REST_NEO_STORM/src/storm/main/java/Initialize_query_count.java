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

@SuppressWarnings("unused")
public class Initialize_query_count{
	//location of the database
	public static final String SERVER_ROOT_URI = "http://localhost:7474/db/data/";
	//location of the graph.dat file
	public static final String DATA_ROOT = "src/storm/main/Resources/graph.dat";
	//name of the index to use
	public static final String NINDEX = "nindex";
	public static final String RINDEX = "rindex";
	//static BufferedWriter bw;
	
	public static void main() throws URISyntaxException {
		//check if database is running or not
		checkDatabaseIsRunning();
		
		//System.out.println("Starting Test . . ");
		final RestAPI api = new RestAPIFacade(SERVER_ROOT_URI);
		//System.out.println("API Started . . ");
        final RestCypherQueryEngine engine = new RestCypherQueryEngine(api);
        //System.out.println("Engine Created . . ");
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
		
		Map<String, Object> counter = new HashMap<String, Object>();
		counter.put("name", "tuplecounter");
		RestNode n_count = api.getOrCreateNode(api.index().forNodes("nindex", MapUtil.stringMap(IndexManager.PROVIDER, "lucene", "type", "fulltext")), "name", "tuplecounter", counter);
		n_count.setProperty("count", 0);
		System.out.println("Counter Initialized to : "+n_count.getProperty("count").toString());
		n_count.setProperty("InitialTime", System.currentTimeMillis());
		System.out.println("Time Initialized : "+n_count.getProperty("InitialTime").toString());
	}
	
	public static void checkDatabaseIsRunning() {
		WebResource resource = Client.create().resource(SERVER_ROOT_URI);
		ClientResponse response = resource.get(ClientResponse.class);
		/*System.out.println(String.format("Server Running Successfully\nStatus Code : [%s]"
				+ "\nLocation : [%s]",response.getStatus(),SERVER_ROOT_URI));*/
	}
}