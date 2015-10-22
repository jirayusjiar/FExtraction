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

import entity.PostEntity;
import entity.ReadabilityEntity;

public class FExtraction {

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

	  try (Connection dbConnection = connectToDB();
			ResultSet rs = executeQuery(dbConnection,
				  "select * from question limit 10");
			PreparedStatement preparedStatement = dbConnection
				  .prepareStatement("INSERT INTO question_features values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")) {

		 dbConnection.setAutoCommit(false);

		 if (rs != null) {

			ExecutorService threadPool = Executors.newFixedThreadPool(64);
			List<Future<ReadabilityEntity>> list = new ArrayList<Future<ReadabilityEntity>>();

			// Process through the resultset of query
			while (rs.next()) {
			   PostEntity postEnt = new PostEntity(rs);
			   Callable<ReadabilityEntity> callable = new ExtractionExecutor(
					 postEnt);
			   list.add(threadPool.submit(callable));
			   System.out.println("Pending id : " + postEnt.id);
			}

			// Insert into db
			int y = 0;
			for (Future<ReadabilityEntity> readEnt : list) {
			   preparedStatement.clearParameters();
			   ReadabilityEntity entToDb = readEnt.get();
			   System.out.println("Preparing Statement for id : " + entToDb.id);
			   preparedStatement.setInt(1, entToDb.id);
			   preparedStatement.setInt(2, entToDb.contentLength);
			   preparedStatement.setDouble(3, entToDb.ari);
			   preparedStatement.setDouble(4, entToDb.colemanIndex);
			   preparedStatement.setDouble(5, entToDb.fleschKincaid);
			   preparedStatement.setDouble(6, entToDb.fleschReading);
			   preparedStatement.setDouble(7, entToDb.gunningFox);
			   preparedStatement.setDouble(8, entToDb.smog);
			   preparedStatement.setInt(9, entToDb.sentenceCount);
			   preparedStatement.setInt(10, entToDb.wordCount);
			   preparedStatement.setInt(11, entToDb.nTag);
			   preparedStatement.setDouble(12, 0.0);
			   preparedStatement.setInt(13, entToDb.loc);
			   preparedStatement.setBoolean(14, entToDb.hasAnswer);
			   preparedStatement.setBoolean(15, entToDb.hasCode);
			   preparedStatement.addBatch();
			}
			preparedStatement.executeBatch();
			dbConnection.commit();
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
