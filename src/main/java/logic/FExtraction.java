package logic;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.TreeMap;

public class FExtraction {
   // Last index is around 26 million == querySize*numIteration>26000000

   // Test
   // private static final int querySize = 10;
   // private static final int numIteration = 2;
   // private static final int numThread = 5;

   private static Map<Integer, Integer> questionDate = new TreeMap<Integer, Integer>();

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

	  execution();
	  for (Integer dateKey : questionDate.keySet())
		 System.out.println(dateKey + "," + questionDate.get(dateKey));
   }

   private static int dateToInt(Date inputDate) {
	  return (inputDate.getYear() + 1900) * 10000 + inputDate.getMonth() * 100
			+ inputDate.getDate();
   }

   // Divide dataset into small size
   private static void execution() {
	  try (Connection dbConnection = connectToDB();
			ResultSet rs = executeQuery(dbConnection,
				  "select creation_date from question");) {

		 if (rs != null) {
			int id;
			while (rs.next()) {
			   int dateIntFormat = dateToInt(rs.getDate("creation_date"));
			   if (questionDate.containsKey(dateIntFormat))
				  questionDate.put(dateIntFormat,
						questionDate.get(dateIntFormat) + 1);
			   else
				  questionDate.put(dateIntFormat, 1);
			}
		 }
	  } catch (SQLException e) {
		 e.printStackTrace();
		 e.getNextException().printStackTrace();
	  }
   }
}
