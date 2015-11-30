package logic;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.HashMap;

public class FExtraction {
   // Last index around 26 million == querySize*numIteration>26000000
   private static final int querySize = 1000000;
   private static final int numIteration = 26;

   private static HashMap<Integer, Integer> acceptedAnsQuestion = new HashMap<Integer, Integer>();
   private static HashMap<Integer, Date> questionTimestamp = new HashMap<Integer, Date>();

   private static PreparedStatement preparedStatement;

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

	  System.out.println("Get question information...");
	  getQuestionInformation();
	  System.out.println("Start the execution...");
	  for (int i = 0; i < numIteration; ++i)
		 execution(i, querySize);

   }

   public static void getQuestionInformation() {
	  try (Connection dbConnection = connectToDB();
			ResultSet rs = executeQuery(
				  dbConnection,
				  "select id,accepted_answer_id,creation_date from posts where accepted_answer_id is not null");) {
		 System.out.println("Finish fetching query\nStart adding executed id");
		 if (rs != null) {
			while (rs.next()) {
			   acceptedAnsQuestion.put(rs.getInt(2), rs.getInt(1));
			   questionTimestamp.put(rs.getInt(1), rs.getTimestamp(3));
			}
		 }
	  } catch (Exception e) {
		 e.printStackTrace();
	  }
   }

   private static void addBatch(int postId, Date questionTimestamp,
		 Date answerTimestamp, int timeDiff) {
	  try {
		 preparedStatement.setInt(1, postId);
		 preparedStatement.setDate(2,
			   new java.sql.Date(questionTimestamp.getTime()));
		 preparedStatement.setDate(3,
			   new java.sql.Date(answerTimestamp.getTime()));
		 preparedStatement.setInt(4, timeDiff);
		 preparedStatement.addBatch();
	  } catch (SQLException e) {
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
				  "SELECT id,creation_date FROM posts WHERE post_type_id = 2 and id >"
						+ (index * querySize) + " and id < "
						+ ((index + 1) * querySize + 1));) {

		 System.out.println("Finish fetching query\nStart processing");
		 if (rs != null) {
			preparedStatement = dbConnection
				  .prepareStatement("INSERT INTO \"timeDiff\" VALUES (?,?,?,?)");

			// Process through the resultset of query
			int postId, timeDiff;
			Date postCreationdate;
			while (rs.next()) {
			   postId = rs.getInt(1);
			   postCreationdate = rs.getTimestamp(2);

			   if (!acceptedAnsQuestion.containsKey(postId)
					 && !questionTimestamp.containsKey(acceptedAnsQuestion
						   .get(postId)))
				  continue;

			   timeDiff = (int) ((postCreationdate.getTime() - questionTimestamp
					 .get(acceptedAnsQuestion.get(postId)).getTime()) / (60 * 1000));

			   addBatch(acceptedAnsQuestion.get(postId),
					 questionTimestamp.get(acceptedAnsQuestion.get(postId)),
					 postCreationdate, timeDiff);

			}
			preparedStatement.executeBatch();
		 }
	  } catch (SQLException e) {
		 e.printStackTrace();
		 e.getNextException().printStackTrace();
	  } finally {
		 try {
			preparedStatement.executeBatch();
			preparedStatement.close();
		 } catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		 }
	  }

	  System.out.println("Done :D");
   }
}
