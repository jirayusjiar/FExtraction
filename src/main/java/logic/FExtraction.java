package logic;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import entity.ParsedTextEntity;
import entity.PostEntity;

public class FExtraction {
   // Last index around 26 million == querySize*numIteration>26000000
   private static final int querySize = 1000000;
   private static final int numIteration = 27;

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
	  System.out.println("Start the execution...");

	  for (int i = 0; i < numIteration; ++i)
		 execution(i, querySize);

   }

   // Divide dataset into small size
   private static void execution(int index, int querySize) {

	  System.out.println("Execution from id >=" + (index * querySize)
			+ " and id < " + ((index + 1) * querySize));
	  try (Connection dbConnection = connectToDB();
			ResultSet rs = executeQuery(dbConnection,
				  "select id,body from question where id >="
						+ (index * querySize) + " and id < "
						+ ((index + 1) * querySize));
			PreparedStatement preparedStatement = dbConnection
				  .prepareStatement("INSERT INTO question_preprocess (id,\"parsedText\") values (?,?)")) {

		 System.out.println("Finish fetching query\nStart processing");
		 if (rs != null) {

			ExecutorService threadPool = Executors.newFixedThreadPool(136);
			List<Future<ParsedTextEntity>> list = new ArrayList<Future<ParsedTextEntity>>();

			// Process through the resultset of query
			while (rs.next()) {
			   PostEntity postEnt = new PostEntity(rs);
			   Callable<ParsedTextEntity> callable = new ExtractionExecutor(
					 postEnt);
			   list.add(threadPool.submit(callable));
			}
			System.out.println("Finish preprocessing\nStart putting to DB");

			if (!list.isEmpty()) {

			   // Insert into db
			   for (Future<ParsedTextEntity> readEnt : list) {
				  preparedStatement.clearParameters();
				  ParsedTextEntity entToDb = readEnt.get();
				  preparedStatement.setInt(1, entToDb.id);
				  preparedStatement.setString(2, entToDb.parseText);
				  preparedStatement.addBatch();
			   }
			   preparedStatement.executeBatch();
			}
		 }
	  } catch (SQLException e) {
		 e.printStackTrace();
		 e.getNextException().printStackTrace();
	  } catch (InterruptedException e) {
		 e.printStackTrace();
	  } catch (ExecutionException e) {
		 e.printStackTrace();
	  }
	  System.out.println("Done :D");
   }
}
