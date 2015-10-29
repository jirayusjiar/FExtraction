package logic;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import edu.stanford.nlp.pipeline.StanfordCoreNLP;

public class FExtraction {
   // Last index is around 26 million == querySize*numIteration>26000000

   // Test
   // private static final int querySize = 10;
   // private static final int numIteration = 2;
   // private static final int numThread = 5;

   // Real
   private static final int querySize = 10000;
   private static final int numIteration = 2600;
   private static final int numThread = 6;

   private static StanfordCoreNLP[] pipeline;
   private static Queue<Integer>[] idQueue;
   private static Queue<String>[] bodyQueue;

   private static Connection connectToDB() {

	  System.out.println("-------- PostgreSQL "
			+ "JDBC Connection Testing ------------");

	  try {

		 Class.forName("org.postgresql.Driver");

	  } catch (ClassNotFoundException e) {

		 System.out.println("Where is your PostgreSQL JDBC Driver? "
			   + "Include in your library path!");
		 e.printStackTrace();
		 return null;

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
		 return null;

	  }

	  if (connection != null) {
		 System.out.println("You made it, take control your database now!");
		 return connection;
	  } else {
		 System.out.println("Failed to make connection!");
		 return null;
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

   public static void main(String[] args) {
	  System.out.println("Start preparing the environment");
	  Properties props = new Properties();
	  props.setProperty("annotators",
			"tokenize, ssplit, pos, lemma, ner, parse, dcoref");
	  pipeline = new StanfordCoreNLP[numThread];
	  idQueue = new Queue[numThread];
	  bodyQueue = new Queue[numThread];
	  for (int i = 0; i < numThread; ++i) {
		 idQueue[i] = new ArrayDeque<Integer>();
		 bodyQueue[i] = new ArrayDeque<String>();
		 pipeline[i] = new StanfordCoreNLP(props);
	  }

	  System.out.println("Start the execution...");
	  for (int i = 0; i < numIteration; ++i)
		 execution(i, querySize);

   }

   private static HashSet<Integer> getCalculated(int index, int querySize) {

	  HashSet<Integer> output = new HashSet<Integer>();
	  System.out.println("Iteration " + (index + 1)
			+ " find calculated from id >=" + (index * querySize)
			+ " and id < " + ((index + 1) * querySize));
	  try (Connection dbConnection = connectToDB();
			ResultSet rs = executeQuery(dbConnection,
				  "select id from question_features where id >="
						+ (index * querySize) + " and id < "
						+ ((index + 1) * querySize)
						+ " and \"politeness\" != 0");) {
		 System.out.println("Iteration " + (index + 1)
			   + " Finish fetching query\nStart adding calculated id");
		 if (rs != null) {
			while (rs.next()) {
			   output.add(rs.getInt(1));
			}
		 }
	  } catch (SQLException e) {
		 e.printStackTrace();
		 e.getNextException().printStackTrace();
	  }

	  return output;
   }

   // Divide dataset into small size
   private static void execution(int index, int querySize) {

	  HashSet<Integer> calculatedId = getCalculated(index, querySize);

	  System.out.println("Iteration " + (index + 1) + " Execution from id >="
			+ (index * querySize) + " and id < " + ((index + 1) * querySize));
	  try (Connection dbConnection = connectToDB();
			ResultSet rs = executeQuery(dbConnection,
				  "select id,\"tokenizedSentence\" from question_preprocess where id >="
						+ (index * querySize) + " and id < "
						+ ((index + 1) * querySize));) {
		 System.out.println("Iteration " + (index + 1)
			   + " Finish fetching query\nStart processing");
		 if (rs != null) {

			// Init threadPool
			ExecutorService threadPool = Executors
				  .newFixedThreadPool(numThread);

			// Submitting the query to executor thread
			int runner = 0;
			while (rs.next()) {
			   if (calculatedId.contains(rs.getInt("id")))
				  continue;
			   idQueue[runner].add(rs.getInt("id"));
			   bodyQueue[runner].add(rs.getString("tokenizedSentence"));
			   ++runner;
			   if (runner == numThread)
				  runner = 0;
			}
			System.out.println("Iteration " + (index + 1)
				  + " Finish preparing all the query");
			for (int i = 0; i < numThread; ++i) {
			   threadPool.execute(new ExtractionExecutor(index + 1, i + 1,
					 idQueue[i], bodyQueue[i], pipeline[i]));
			}
			System.out.println("Iteration " + (index + 1)
				  + " Finish submit to executors");

			threadPool.shutdown();
			while (!threadPool.awaitTermination(24L, TimeUnit.HOURS)) {
			}
			System.out.println("Iteration " + (index + 1)
				  + " Finish preprocessing\n");

		 }
	  } catch (SQLException e) {
		 e.printStackTrace();
		 e.getNextException().printStackTrace();
	  } catch (InterruptedException e) {
		 e.printStackTrace();
	  }
	  System.out.println("Iteration " + (index + 1)
			+ " Iteration execution DONE :D");
   }
}
