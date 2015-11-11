package logic;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.codec.binary.Base64;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import entity.ParsedTextEntity;

public class FExtraction {
   // Last index is around 26 million == querySize*numIteration>26000000

   // Test
   // private static final int querySize = 100;
   // private static final int numIteration = 130000;
   // private static final int numThread = 2;

   // Real
   private static int numThread;

   private static StanfordCoreNLP[] pipeline;
   private static Queue<Integer>[] idQueue;
   private static Queue<String>[] bodyQueue;
   private static Gson gson;

   private static void checkDBConnection() throws SQLException,
		 ClassNotFoundException {

	  System.out.println("-------- PostgreSQL "
			+ "JDBC Connection Testing ------------");

	  try {

		 Class.forName("org.postgresql.Driver");

	  } catch (ClassNotFoundException e) {

		 System.out.println("Where is your PostgreSQL JDBC Driver? "
			   + "Include in your library path!");
		 e.printStackTrace();
		 throw e;

	  }

	  System.out.println("PostgreSQL JDBC Driver Registered!");

	  Connection connection = null;

	  try {

		 connection = DriverManager.getConnection(
			   "jdbc:postgresql://163.221.172.217:5432/so_msr2015", "oss",
			   "kenjiro+");

	  } catch (SQLException e) {

		 System.out.println("Connection Failed! Check output console");
		 e.printStackTrace();
		 throw e;

	  }

	  if (connection != null) {
		 System.out.println("You made it, take control your database now!");
	  } else {
		 System.out.println("Failed to make connection!");
		 throw new SQLException("Failed to make connection");
	  }

   }

   private static ResultSet executeQuery(Connection conn, String query) {

	  try {
		 return conn.createStatement().executeQuery(query);
	  } catch (SQLException e) {
		 System.out.println();
		 e.printStackTrace();
		 return null;
	  }

   }

   public static void main(String[] args) throws ClassNotFoundException,
		 SQLException {
	  numThread = Integer.parseInt(args[0]);
	  System.out.println("Number of thread : " + args[0]);
	  System.out.println("Checking connection to DB");
	  checkDBConnection();

	  System.out.println("Start preparing the environment");
	  Properties props = new Properties();
	  props.setProperty("annotators",
			"tokenize, ssplit, pos, lemma, ner, parse, dcoref");
	  gson = new Gson();

	  pipeline = new StanfordCoreNLP[numThread];
	  idQueue = new Queue[numThread];
	  bodyQueue = new Queue[numThread];
	  for (int i = 0; i < numThread; ++i) {
		 idQueue[i] = new ArrayDeque<Integer>();
		 bodyQueue[i] = new ArrayDeque<String>();
		 pipeline[i] = new StanfordCoreNLP(props);
	  }

	  System.out.println("Start the execution...");
	  List<ParsedTextEntity> listToExecute = new ArrayList<ParsedTextEntity>();
	  while (true) {
		 if (listToExecute.size() == 0) {
			listToExecute = getListToExecute();
			if (listToExecute == null)
			   break;
		 }
		 System.out.println("Finish getting the list\nStart processing");
		 for (int x = 0; x < listToExecute.size(); ++x) {
			idQueue[x % numThread].add(listToExecute.get(x).id);
			bodyQueue[x % numThread].add(listToExecute.get(x).parseText);
		 }
		 execute();
		 System.out.println("Finish execution");
		 try {
			Thread.sleep(100);
		 } catch (InterruptedException e) {
			e.printStackTrace();
		 }
	  }

   }

   private static List<ParsedTextEntity> getListToExecute() {

	  Document doc;
	  try {
		 doc = Jsoup.connect("http://163.221.172.217:8080").get();
		 String[] fetchedText = doc.text().split(" ");

		 System.out.println("Now extracting " + fetchedText[0] + " from "
			   + fetchedText[1]);
		 byte[] decodedBytes = Base64.decodeBase64(fetchedText[2]);
		 String decodedString = new String(decodedBytes);
		 return gson.fromJson(decodedString,
			   new TypeToken<List<ParsedTextEntity>>() {
			   }.getType());

	  } catch (IOException e) {
		 e.printStackTrace();
		 return null;
	  }
   }

   // Divide dataset into small size
   private static void execute() {

	  // Init threadPool
	  ExecutorService threadPool = Executors.newFixedThreadPool(numThread);
	  for (int i = 0; i < numThread; ++i) {
		 threadPool.execute(new ExtractionExecutor(i + 1, idQueue[i],
			   bodyQueue[i], pipeline[i]));
	  }
	  System.out.println("Finish submit data to executors");

	  threadPool.shutdown();
	  try {
		 while (!threadPool.awaitTermination(24L, TimeUnit.HOURS)) {
		 }
	  } catch (InterruptedException e) {
		 e.printStackTrace();
	  }
   }
}
