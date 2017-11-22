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
import com.couchbase.client.java.query.AsyncN1qlQueryRow;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryResult;
import com.couchbase.client.java.query.N1qlQueryRow;
import com.couchbase.client.java.query.Statement;
import com.couchbase.client.java.search.SearchQuery;
import com.couchbase.client.java.search.queries.MatchQuery;
import com.couchbase.client.java.search.result.SearchQueryResult;
import com.couchbase.client.java.search.result.SearchQueryRow;
import com.couchbase.client.java.subdoc.DocumentFragment;

import rx.Observable;

import static com.couchbase.client.java.query.Select.select;
import static com.couchbase.client.java.query.dsl.Expression.*;

public class MainLab {
	
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

	}
	
	private static void process(String cmdLn) {
		String words[] = cmdLn.split(" ");
		String key;
		
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

	}

	private static void read(String[] words) {

	}
	
	private static void update(String[] words) {

	}
	
	private static void subdoc(String[] words) {

	}

	private static void delete(String[] words) {

	}
	
	private static void query(String[] words) {

	}

	private static void queryAsync(String[] words) {

	}
	
	private static void queryAirports(String[] words) {

	}
	
	private static void bulkWrite(String[] words) {

	}

	private static void bulkWriteSync(String[] words) {

	}
	
	private static void search(String[] words) {

	}
	
	private static void usage() {
		System.out.println("usage : " + CMD_CREATE + " [key from to] | " + CMD_READ + " [key] | " 
				+ CMD_UPDATE + " [airline_key] | " + CMD_SUBDOC + " [msg_key] | \n" + CMD_DELETE + " [msg_key] | " 
				+ CMD_QUERY + " | " + CMD_QUERY_AIRPORTS + " [sourceairport destinationairport] | "
				+ CMD_QUERY_ASYNC +  " | \n" + CMD_BULK_WRITE + " [size] | " + CMD_BULK_WRITE_SYNC + " [size] | "
				+ CMD_SEARCH + " [term] | "+ CMD_QUIT);		
	}

}
