package logic;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import entity.VoteInstance;

public class FExtraction {
   // Last index around 26 million == querySize*numIteration>26000000
   private static final int querySize = 1000000;
   private static final int numIteration = 26;

   private static HashMap<Integer, Map<Date, VoteInstance>> uidTimestamp = new HashMap<Integer, Map<Date, VoteInstance>>();
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

	  System.out.println("Get user information...");
	  getUserInformation();
	  System.out.println("Start the execution...");
	  for (int i = 0; i < numIteration; ++i)
		 execution(i, querySize);

   }

   public static void getUserInformation() {
	  try (Connection dbConnection = connectToDB();
			ResultSet rs = executeQuery(dbConnection,
				  "SELECT \"timeStamp\", \"userId\", value, \"voteType\" FROM \"VoteHistory\"");) {
		 System.out.println("Finish fetching query\nStart adding executed id");
		 int uid, val, voteType;
		 Date timestamp;
		 if (rs != null) {
			while (rs.next()) {
			   timestamp = rs.getDate(1);
			   uid = rs.getInt(2);
			   val = rs.getInt(3);
			   voteType = rs.getInt(4);

			   if (!uidTimestamp.containsKey(uid))
				  uidTimestamp.put(uid, new TreeMap<Date, VoteInstance>());

			   if (uidTimestamp.get(uid).containsKey(timestamp)) {
				  uidTimestamp.get(uid).get(timestamp).add(voteType, val);
			   } else {
				  uidTimestamp.get(uid).put(timestamp,
						new VoteInstance(voteType, val));
			   }

			}
		 }
	  } catch (Exception e) {
		 e.printStackTrace();
	  }
   }

   private static void addBatch(VoteInstance inputVoteInstance, int postId) {
	  try {
		 for (int x = 1; x < 13; ++x) {
			preparedStatement.setInt(x, inputVoteInstance.VoteType[x - 1]);
		 }
		 preparedStatement.setInt(13, inputVoteInstance.VoteType[14]);
		 preparedStatement.setInt(14, inputVoteInstance.VoteType[15]);
		 preparedStatement.setInt(15, postId);

		 preparedStatement.addBatch();
	  } catch (SQLException e) {
		 e.printStackTrace();
	  }

   }

   private static VoteInstance getSummaryVoteInstance(Date postCreationDate,
		 Map<Date, VoteInstance> userLog) {
	  // Dummy VoteInstance : 0 votes 0 reputation
	  VoteInstance output = new VoteInstance(14, 0);

	  for (Map.Entry<Date, VoteInstance> entry : userLog.entrySet()) {
		 if (entry.getKey().before(postCreationDate)) {
			// If this is before the post creation
			output.add(entry.getValue());
		 }
	  }

	  return output;
   }

   // Divide dataset into small size
   private static void execution(int index, int querySize) {

	  String[] tmpStringArr;
	  Integer[] sentimentalScore = new Integer[2];

	  System.out.println("Execution from id >=" + (index * querySize)
			+ " and id < " + ((index + 1) * querySize));
	  try (Connection dbConnection = connectToDB();
			ResultSet rs = executeQuery(dbConnection,
				  "SELECT id, creation_date, owner_user_id FROM question WHERE id >"
						+ (index * querySize) + " and id < "
						+ ((index + 1) * querySize));) {

		 System.out.println("Finish fetching query\nStart processing");
		 if (rs != null) {
			preparedStatement = dbConnection
				  .prepareStatement("UPDATE question_features SET "
						+ "\"acceptedAnswer\"=?, \"downVote\"=?, \"offensiveVote\"=?, "
						+ "\"favoriteVote\"=?, \"closeVote\"=?, \"reopenVote\"=?, "
						+ "\"bountyStartVote\"=?,  \"bountyCloseVote\"=?, "
						+ "\"deletionVote\"=?, \"undeletionVote\"=?, \"spamVote\"=?, "
						+ "\"moderatorReviewVote\"=?, \"approveEditVote\"=? WHERE id=?");

			// Process through the resultset of query
			int postId, userId, reputation;
			int[] voteSum;
			Date creationDate;
			VoteInstance tmpVoteInstance;
			while (rs.next()) {
			   // Dummy VoteInstance : 0 votes 0 reputation
			   tmpVoteInstance = new VoteInstance(14, 0);
			   postId = rs.getInt(1);
			   creationDate = rs.getDate(2);
			   userId = rs.getInt(3);

			   if (uidTimestamp.containsKey(userId)) {
				  tmpVoteInstance = getSummaryVoteInstance(creationDate,
						uidTimestamp.get(userId));
			   }
			   addBatch(tmpVoteInstance, postId);
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
