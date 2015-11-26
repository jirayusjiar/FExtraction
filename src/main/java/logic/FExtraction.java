package logic;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

public class FExtraction {
   // Last index around 75 million == querySize*numIteration>75000000
   private static final int querySize = 1000000;
   private static final int numIteration = 75;

   private static HashMap<Integer, Integer> questionToAskerId = new HashMap<Integer, Integer>();
   private static PreparedStatement preparedStatement;
   private static int count = 1;

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
			ResultSet rs = executeQuery(dbConnection,
				  "select id,owner_user_id from posts where post_type_id = 1");) {
		 System.out.println("Finish fetching query\nStart adding executed id");
		 if (rs != null) {
			while (rs.next()) {
			   questionToAskerId.put(rs.getInt(1), rs.getInt(2));
			}
		 }
	  } catch (Exception e) {
		 e.printStackTrace();
	  }
   }

   private static void addBatch(Date voteDate, int userId, int value,
		 int voteType) {
	  try {
		 preparedStatement.clearBatch();
		 preparedStatement.setInt(1, count);
		 preparedStatement.setDate(2, voteDate);
		 preparedStatement.setInt(3, userId);
		 preparedStatement.setInt(4, value);
		 preparedStatement.setInt(5, voteType);
		 preparedStatement.addBatch();
		 ++count;
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
			ResultSet rs = executeQuery(
				  dbConnection,
				  "SELECT votes.vote_type_id, votes.creation_date, votes.user_id, votes.bounty_amount, posts.parent_id, posts.owner_user_id, posts.last_editor_user_id,posts.post_type_id FROM votes, posts WHERE votes.post_id = posts.id and votes.id >"
						+ (index * querySize)
						+ " and votes.id < "
						+ ((index + 1) * querySize));) {

		 System.out.println("Finish fetching query\nStart processing");
		 if (rs != null) {
			preparedStatement = dbConnection
				  .prepareStatement("INSERT INTO  VoteHistory VALUES (?,?,?,?,?)");

			// Process through the resultset of query
			int voteType, voteUserId, bountyAmount, postParentId, postUserId, postLastEditUserId, postTypeId;
			Date voteDate;
			while (rs.next()) {
			   voteType = rs.getInt(1);
			   voteDate = rs.getDate(2);
			   voteUserId = rs.getInt(3);
			   bountyAmount = rs.getInt(4);
			   postParentId = rs.getInt(5);
			   postUserId = rs.getInt(6);
			   postLastEditUserId = rs.getInt(7);
			   postTypeId = rs.getInt(8);

			   switch (voteType) {
			   case 1:
				  // AcceptedAnswer Vote Type
				  // +15 to answerer
				  addBatch(voteDate, postUserId, 15, voteType);
				  // +2 to questioner
				  if (questionToAskerId.containsKey(postParentId))
					 addBatch(voteDate, questionToAskerId.get(postParentId), 2,
						   voteType);
				  break;
			   case 2:
				  // Upvote
				  // +5 if the post is answer
				  if (postTypeId == 2)
					 addBatch(voteDate, postUserId, 5, voteType);
				  else
					 // +10 if the post is question
					 addBatch(voteDate, postUserId, 5, voteType);
				  break;
			   case 3:
				  // Downvote
				  // -2 to owner of the post
				  addBatch(voteDate, postUserId, -2, voteType);
				  break;
			   case 8:
				  // BountyStart
				  // -BountyAmount to user who start it
				  addBatch(voteDate, voteUserId, -bountyAmount, voteType);
				  break;
			   case 9:
				  // BountyEnd
				  // +BountyAmount to user who got it
				  addBatch(voteDate, voteUserId, bountyAmount, voteType);
				  break;
			   case 16:
				  // EditSuggestApproved
				  // +2 to that user
				  addBatch(voteDate, postLastEditUserId, 2, voteType);
				  break;
			   default:
				  // Case Offensive, Favorite, Close, Reopen, Deletion,
				  // Undeletion, Spam and ModReview
				  addBatch(voteDate, postUserId, 0, voteType);
				  break;
			   }

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
