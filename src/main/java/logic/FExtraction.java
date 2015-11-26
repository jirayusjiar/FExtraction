package logic;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;

import uk.ac.wlv.sentistrength.SentiStrength;

public class FExtraction {
   // Last index around 26 million == querySize*numIteration>26000000
   private static final int querySize = 1000000;
   private static final int numIteration = 27;

   private static HashSet<Integer> targetId = new HashSet<Integer>();
   private static SentiStrength classifierModel = new SentiStrength();

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

	  String ssthInitialisationAndText[] = { "sentidata",
			"resource/SentiStrength/SentStrength_Data_Sept2011/" };

	  classifierModel = new SentiStrength();
	  classifierModel.initialise(ssthInitialisationAndText);

	  System.out.println("Get targetId...");
	  getTargetId();
	  System.out.println("Start the execution...");
	  for (int i = 0; i < numIteration; ++i)
		 execution(i, querySize);

   }

   public static void getTargetId() {
	  try (Connection dbConnection = connectToDB();
			ResultSet rs = executeQuery(dbConnection,
				  "select id from question_features where \"sentimentalTotal\" is null");) {
		 System.out.println("Finish fetching query\nStart adding executed id");
		 if (rs != null) {
			while (rs.next()) {
			   targetId.add(rs.getInt("id"));
			}
		 }
	  } catch (Exception e) {
		 e.printStackTrace();
	  }
   }

   // Divide dataset into small size
   private static void execution(int index, int querySize) {

	  String[] tmpStringArr;
	  Integer[] sentimentalScore = new Integer[2];

	  System.out.println("Execution from id >=" + (index * querySize)
			+ " and id < " + ((index + 1) * querySize));
	  try (Connection dbConnection = connectToDB();
			ResultSet rs = executeQuery(dbConnection,
				  "select id,\"tokenizedSentence\" from question_preprocess where id >="
						+ (index * querySize) + " and id < "
						+ ((index + 1) * querySize));
			PreparedStatement preparedStatement = dbConnection
				  .prepareStatement("UPDATE question_features set \"sentimentalPositive\" = ?, \"sentimentalNegative\" = ?, \"sentimentalTotal\" = ? where id = ?")) {

		 System.out.println("Finish fetching query\nStart processing");
		 if (rs != null) {

			// Process through the resultset of query
			while (rs.next()) {
			   if (!targetId.contains(rs.getInt(1)))
				  continue;

			   tmpStringArr = classifierModel.computeSentimentScores(
					 rs.getString(2)).split(" ");

			   for (int x = 0; x < 2; ++x)
				  sentimentalScore[x] = Integer.parseInt(tmpStringArr[x]);

			   preparedStatement.clearParameters();
			   preparedStatement.setInt(1, sentimentalScore[0]);
			   preparedStatement.setInt(2, sentimentalScore[1]);
			   preparedStatement.setInt(3, sentimentalScore[0]
					 + sentimentalScore[1]);
			   preparedStatement.setInt(4, rs.getInt(1));
			   preparedStatement.addBatch();

			}
			preparedStatement.executeBatch();
		 }
	  } catch (SQLException e) {
		 e.printStackTrace();
		 e.getNextException().printStackTrace();
	  }

	  System.out.println("Done :D");
   }
}
