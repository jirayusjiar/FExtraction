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

public class FExtraction {
   // Last index around 26 million == querySize*numIteration>26000000
   private static final int querySize = 1000000;
   private static final int numIteration = 12;
   private static final String[] questionBadges = new String[] { "Altruist",
		 "Benefactor", "Curious", "Inquisitive", "Socratic",
		 "Favorite Question", "Stellar Question", "Investor", "Nice Question",
		 "Good Question", "Great Question", "Popular Question",
		 "Notable Question", "Famous Question", "Promoter", "Scholar",
		 "Student", "Tumbleweed" };

   private static final String[] answerBadges = new String[] { "Enlightened",
		 "Explainer", "Refiner", "Illuminator", "Generalist", "Guru",
		 "Nice Answer", "Good Answer", "Great Answer", "Populist", "Reversal",
		 "Revival", "Necromancer", "Self-Learner", "Teacher", "Tenacious",
		 "Unsung Hero" };

   private static HashMap<Integer, Map<Date, Integer[]>> uidTimestamp = new HashMap<Integer, Map<Date, Integer[]>>();
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

   private static int getBadgeType(String badgeName) {
	  // Badge type
	  // 0 Question
	  // 1 Answer
	  // 2 None
	  for (int x = 0; x < questionBadges.length; ++x)
		 if (badgeName.equals(questionBadges[x]))
			return 0;

	  for (int x = 0; x < answerBadges.length; ++x)
		 if (badgeName.equals(answerBadges[x]))
			return 1;

	  return 2;
   }

   public static void getUserInformation() {
	  try (Connection dbConnection = connectToDB();
			ResultSet rs = executeQuery(dbConnection,
				  "SELECT user_id,name,date from badges");) {
		 System.out.println("Finish fetching query\nStart adding executed id");
		 int uid, index;
		 String badgeName;
		 Date timestamp;
		 if (rs != null) {
			while (rs.next()) {
			   badgeName = rs.getString(2);
			   index = getBadgeType(badgeName);

			   if (index == 2)
				  continue;

			   timestamp = rs.getDate(3);
			   uid = rs.getInt(1);

			   if (!uidTimestamp.containsKey(uid))
				  uidTimestamp.put(uid, new TreeMap<Date, Integer[]>());

			   if (!uidTimestamp.get(uid).containsKey(timestamp))
				  uidTimestamp.get(uid).put(timestamp, new Integer[] { 0, 0 });

			   ++uidTimestamp.get(uid).get(timestamp)[index];

			}
		 }
	  } catch (Exception e) {
		 e.printStackTrace();
	  }
   }

   private static void addBatch(Integer[] badgeCount, int postId) {
	  try {
		 preparedStatement.setInt(1, badgeCount[0] + badgeCount[1]);
		 preparedStatement.setInt(2, badgeCount[1]);
		 preparedStatement.setInt(3, badgeCount[0]);
		 preparedStatement.setInt(4, postId);

		 preparedStatement.addBatch();
	  } catch (SQLException e) {
		 e.printStackTrace();
	  }

   }

   private static Integer[] getSummaryVoteInstance(Date postCreationDate,
		 Map<Date, Integer[]> userLog) {

	  Integer[] output = new Integer[] { 0, 0 };

	  for (Map.Entry<Date, Integer[]> entry : userLog.entrySet()) {
		 if (entry.getKey().before(postCreationDate)) {
			// If this is before the post creation
			output[0] += entry.getValue()[0];
			output[1] += entry.getValue()[1];
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
				  .prepareStatement("UPDATE question_features SET \"totalBadge\"=?, "
						+ "\"answerBadge\"=?, \"questionBadge\"=? WHERE id=?");

			// Process through the resultset of query
			int postId, userId, reputation;
			Integer[] badgeSum = null;
			Date creationDate;
			while (rs.next()) {

			   postId = rs.getInt(1);
			   creationDate = rs.getDate(2);
			   userId = rs.getInt(3);

			   if (uidTimestamp.containsKey(userId)) {
				  badgeSum = getSummaryVoteInstance(creationDate,
						uidTimestamp.get(userId));
			   }
			   addBatch(badgeSum, postId);
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
