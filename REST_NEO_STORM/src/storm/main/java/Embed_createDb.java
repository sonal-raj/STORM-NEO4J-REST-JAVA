/*
 * Program to read from a file and write to a database
 */

package storm.main.java;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.*;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.graphdb.index.UniqueFactory;


public class Embed_createDb {
	//Path to neo4j
	private static final String neo4j_dbpath="C:/Users/sonalraj/Desktop/neo4jdb_trial";
	Node first;
	Node second;
	Relationship relation;
	GraphDatabaseService graphDataService = new GraphDatabaseFactory().newEmbeddedDatabase(neo4j_dbpath);
	
	//Begin Transaction
	Transaction transaction = graphDataService.beginTx();
	
	//Initiate the indexing service
	IndexManager index = graphDataService.index();
	Index<Node> nodes = index.forNodes("nodes");
	
	//list of relationships : first knows second
	private static enum RelTypes implements RelationshipType
	{
		LINKS
	}
	
	//###ANALYTICS-TIME###
	long file_index=2;
	long node_count=0;
	long startTime = System.currentTimeMillis();
	static FileOutputStream fop =null;
	//###ANALYTICS-TIME###
	
	void createRelationship(final String fromNodeName, final String toNodeName, final String propertyName, final Object propertyValue)
	{	
		try{
			Double d = Double.parseDouble(propertyValue.toString());
			//create node and set the properties
			first = getOrCreateUserWithUniqueFactory(fromNodeName, graphDataService);
			nodes.add(first, "name", first.getProperty("name"));
			second = getOrCreateUserWithUniqueFactory(toNodeName, graphDataService);
			nodes.add(second, "name", second.getProperty("name"));
			//relationship
			relation = first.createRelationshipTo(second, RelTypes.LINKS);
			relation.setProperty(propertyName, d);
			System.out.println("Node "+fromNodeName+"->"+toNodeName+" added with Distance : "+propertyValue);
			
			//###ANALYTICS###
			//create the tuple of <nodes,time> and write to files
			node_count++;
			StringBuilder tups = new StringBuilder();
			tups.append(node_count);
			tups.append(",");
			tups.append((System.currentTimeMillis()-startTime));
			
			/*
			 * Write Code to Write to File "create-nodes-time-embedded.dat"
			 */
			byte[] contentInBytes = tups.toString().getBytes();
			fop.write(contentInBytes);
			fop.write(System.getProperty("line.separator").getBytes());
			fop.flush();
			//###ANALYTICS###
			
			}
		catch(Exception e) {System.out.println("Exception Occurred!!");}
	}
	
	void createDatabase()
	{
		try(BufferedReader br = new BufferedReader(new FileReader("src/storm/main/Resources/graph.dat")))
		{
			String sCurrLine;
			while((sCurrLine = br.readLine())!=null)
			{
				String[] fields = sCurrLine.split(" ");
				if(fields[0].equals("a"))
				{
					createRelationship(fields[1],fields[2],"cost",fields[3]);
				}
			}
			System.out.println("Database Created!!");
			//sample query and transaction
			
		}catch(IOException e) {
			System.out.println("Database not created!!!");
			e.printStackTrace();
		}	
		transaction.success();
		transaction.finish();
	}
	
	public Node getOrCreateUserWithUniqueFactory( String nodename, GraphDatabaseService graphDb )
	{
	    UniqueFactory<Node> factory = new UniqueFactory.UniqueNodeFactory( graphDb, "nodes" )
	    {
	        @Override
	        protected void initialize(Node created, Map<String, Object> properties)
	        {
	            created.setProperty( "name", properties.get( "name" ) );
	        }
	    };
	    
	 
	    return factory.getOrCreate( "name", nodename );
	}
	
	
	void removeData(Node a, Node b)
	{   /*
		 *Redundant function at this point;To delete the whole Database call it iteratively
		 */
		Transaction transaction = graphDataService.beginTx();
		try {
			//delete a relationship
			a.getSingleRelationship(RelTypes.LINKS, Direction.OUTGOING).delete();
			System.out.println("Nodes are removed !");
			//delete the nodes
			a.delete();
			b.delete();
		}
		finally{
			//finish the transactions
			transaction.finish();
		}
	}
	
	void shutDown()
	{
		//shut down
		graphDataService.shutdown();
		System.out.println("Neo4j Database is shutdown!!");
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		File file=new File("src/storm/main/Outputs/create-nodes-time-embedded.dat");
		try{
			fop =new FileOutputStream(file);
			}
		catch(Exception e) {
				System.out.println("Exception Occurred!!!");
			}
		Embed_createDb hello = new Embed_createDb();
		hello.createDatabase();
		//hello.removeData();
		hello.shutDown();
	}
}
