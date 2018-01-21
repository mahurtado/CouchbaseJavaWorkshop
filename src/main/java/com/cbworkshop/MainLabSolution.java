package com.cbworkshop;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import com.couchbase.client.core.message.kv.subdoc.multi.Mutation;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryResult;
import com.couchbase.client.java.query.N1qlQueryRow;
import com.couchbase.client.java.search.SearchQuery;
import com.couchbase.client.java.search.queries.MatchQuery;
import com.couchbase.client.java.search.result.SearchQueryResult;
import com.couchbase.client.java.search.result.SearchQueryRow;
import com.couchbase.client.java.subdoc.DocumentFragment;

import rx.Observable;

import static com.couchbase.client.java.query.Select.select;

public class MainLabSolution {
	
	public static final String CMD_QUIT = "quit";
	public static final String CMD_CREATE = "create";
	public static final String CMD_READ = "read";
	public static final String CMD_UPDATE = "update";
	public static final String CMD_SUBDOC = "subdoc";
	public static final String CMD_DELETE = "delete";
	public static final String CMD_QUERY = "query";
	public static final String CMD_QUERY_ASYNC = "queryasync";
	public static final String CMD_QUERY_AIRPORTS = "queryairports";
	public static final String CMD_BULK_WRITE = "bulkwrite";
	public static final String CMD_BULK_WRITE_SYNC = "bulkwritesync";
	public static final String CMD_SEARCH = "search";
	
	private static Bucket bucket = null;

	public static void main(String[] args) { 
		Scanner scanner = new Scanner(System.in);
		initConnection();
		welcome();
		usage();
		String cmdLn = null;
		while(!CMD_QUIT.equalsIgnoreCase(cmdLn)){
			try {
				System.out.print("# ");
				cmdLn = scanner.nextLine();
				process(cmdLn);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		scanner.close();
	}

	private static void initConnection(){
		String clusterAddress = System.getProperty("cbworkshop.clusteraddress");
		String user = System.getProperty("cbworkshop.user");
		String password = System.getProperty("cbworkshop.password");
		String bucketName = System.getProperty("cbworkshop.bucket");
		
		CouchbaseEnvironment env = DefaultCouchbaseEnvironment.builder()
                .socketConnectTimeout(15000)
                .connectTimeout(15000) 
                .kvTimeout(15000)
                .build();
	
		Cluster cluster = CouchbaseCluster.create(env, clusterAddress);
		cluster.authenticate(user, password);
		bucket = cluster.openBucket(bucketName);
	}
	
	private static void process(String cmdLn) {
		String words[] = cmdLn.split(" ");
		
		switch(words[0].toLowerCase()){
		case CMD_QUIT:
			System.out.println("bye!");
			break;
		case CMD_CREATE:
			create(words);
			break;
		case CMD_READ:
			read(words);
			break;
		case CMD_UPDATE:
			update(words);
			break;
		case CMD_SUBDOC:
			subdoc(words);
			break;
		case CMD_DELETE:
			delete(words);
			break;
		case CMD_QUERY:
			query(words);
			break;
		case CMD_QUERY_ASYNC:
			queryAsync(words);
			break;
		case CMD_QUERY_AIRPORTS:
			queryAirports(words);
			break;
		case CMD_BULK_WRITE:	
			bulkWrite(words);
			break;
		case CMD_BULK_WRITE_SYNC:	
			bulkWriteSync(words);
			break;
		case CMD_SEARCH:	
			search(words);
			break;
		case "":
			// do nothing
			break;
		default:
			usage();					
		}
	}

	private static void create(String[] words) {
		String key = "msg::" + words[1];
		String from = words[2];
		String to = words[3];
		JsonObject json = JsonObject.create()
				.put("timestamp", System.currentTimeMillis())
			    .put("from", from)
			    .put("to", to)
			    .put("type", "msg");
		bucket.insert(JsonDocument.create(key, json));
		//bucket.upsert(JsonDocument.create(key, json));
		System.out.println("Document created with key: " + key);
	}

	private static void read(String[] words) {
		String key = words[1];
		JsonDocument doc = bucket.get(key);
		System.out.println(doc.content().toString());
	}
	
	private static void update(String[] words) {
		String key = "airline_" + words[1];
		JsonDocument doc = bucket.get(key);
		String name = doc.content().getString("name");
		doc.content().put("name", name.toUpperCase());
		bucket.replace(doc);
	}
	
	private static void subdoc(String[] words) {
		String key = "msg::" + words[1];
		DocumentFragment<Mutation> result = bucket
				.mutateIn(key)
				.replace("from", "Administrator")
				.insert("reviewed", System.currentTimeMillis())
				.execute();
	}

	private static void delete(String[] words) {
		String key = "msg::" + words[1];
		bucket.remove(key);
	}
	
	private static void query(String[] words) {
		//N1qlQueryResult queryResult = bucket.query(N1qlQuery.simple("SELECT * FROM `travel-sample` LIMIT 10"));
		N1qlQueryResult queryResult = bucket.query(select("*").from("`travel-sample`").limit(10));
		for(N1qlQueryRow row : queryResult){
			System.out.println(row.value().toString());
		}
	}

	private static void queryAsync(String[] words) {

		bucket.async()
			.query(N1qlQuery.simple(select("*").from("`travel-sample`").limit(5)))
			.subscribe(result -> {
				result.errors()
					.subscribe(
							e -> System.err.println("N1QL Error/Warning: " + e),
							runtimeError -> runtimeError.printStackTrace()
					);
				result.rows()
					.map(row -> row.value())
					.subscribe(
							rowContent -> System.out.println(rowContent),
							runtimeError -> runtimeError.printStackTrace()
					);
			});
	}
	
	private static void queryAirports(String[] words) {
		String sourceairport = words[1];
		String destinationairport = words[2];
		String queryStr = "SELECT a.name FROM `travel-sample` r JOIN `travel-sample` a ON KEYS r.airlineid " +
				"WHERE r.type=\"route\" AND r.sourceairport=$src AND r.destinationairport=$dst";
		
		JsonObject params = JsonObject.create()
				.put("src", sourceairport)
				.put("dst", destinationairport);
		
		N1qlQuery query = N1qlQuery.parameterized(queryStr, params);
		
		N1qlQueryResult queryResult = bucket.query(query);
		for(N1qlQueryRow row : queryResult){
			System.out.println(row.value().toString());
		}
	}
	
	private static void bulkWrite(String[] words) {
		
		int size = Integer.parseInt(words[1]);
		
		System.out.println("Deleting messages ..." );
		bucket.query(N1qlQuery.simple("DELETE FROM `travel-sample` WHERE type=\"msg\""));
		
		System.out.println("Writting " +size  + " messages");
		List<JsonDocument> docs = new ArrayList<JsonDocument>();
		for(int i = 0; i < size; i++){
			JsonObject json = JsonObject.create()
					.put("timestamp", System.currentTimeMillis())
				    .put("from", "me")
				    .put("to", "you")
				    .put("type", "msg");
			docs.add(JsonDocument.create("msg::" + i, json));
		}
		long ini = System.currentTimeMillis();
		Observable				
		   .from(docs)			    
		   .flatMap(doc -> bucket.async().insert(doc))	
		   .last()
		   .toBlocking()
		   .single();
		System.out.println("Time elapsed " + (System.currentTimeMillis() - ini) + " ms");
	}

	private static void bulkWriteSync(String[] words) {
		
		int size = Integer.parseInt(words[1]);
		
		System.out.println("Deleting messages ..." );
		bucket.query(N1qlQuery.simple("DELETE FROM `travel-sample` WHERE type=\"msg\""));
		
		System.out.println("Writting " +size  + " messages");
		List<JsonDocument> docs = new ArrayList<JsonDocument>();
		for(int i = 0; i < size; i++){
			JsonObject json = JsonObject.create()
					.put("timestamp", System.currentTimeMillis())
				    .put("from", "me")
				    .put("to", "you")
				    .put("type", "msg");
			docs.add(JsonDocument.create("msg::" + i, json));
		}
		long ini = System.currentTimeMillis();
		for(JsonDocument doc : docs){
			bucket.insert(doc);
		}
		System.out.println("Time elapsed " + (System.currentTimeMillis() - ini) + " ms");
	}
	
	private static void search(String[] words) {
		String term = words[1];
		MatchQuery fts = SearchQuery.match(term);
		SearchQueryResult result = bucket.query(new SearchQuery("sidx_hotel_desc", fts));
		for (SearchQueryRow row : result) {
		    System.out.println(row);
		}
	}
	
	
	private static void welcome() {
		System.out.println("Welcome to CouchbaseJavaWorkshop!");
	}
	
	private static void usage() {
		System.out.println("Usage options: \n\n" + CMD_CREATE + " [key from to] \n" + CMD_READ + " [key] \n" 
				+ CMD_UPDATE + " [airline_key] \n" + CMD_SUBDOC + " [msg_key] \n" + CMD_DELETE + " [msg_key] \n" 
				+ CMD_QUERY + " \n" + CMD_QUERY_AIRPORTS + " [sourceairport destinationairport] \n"
				+ CMD_QUERY_ASYNC +  " \n" + CMD_BULK_WRITE + " [size] \n" + CMD_BULK_WRITE_SYNC + " [size] \n"
				+ CMD_SEARCH + " [term] \n"+ CMD_QUIT);		
	}

}
