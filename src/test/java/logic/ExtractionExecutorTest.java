package logic;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.junit.Test;

import uk.ac.wlv.sentistrength.SentiStrength;

public class ExtractionExecutorTest {

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

   @Test
   public void test() {
	  String ssthInitialisationAndText[] = { "sentidata",
			"resource/SentiStrength/SentStrength_Data_Sept2011/" };

	  String ssthInitialisationAndText2[] = { "sentidata",
			"resource/SentiStrength/SentStrength_Data_Sept2011/", "explain" };

	  SentiStrength classifier = new SentiStrength();
	  classifier.initialise(ssthInitialisationAndText);

	  SentiStrength classifier2 = new SentiStrength();
	  classifier2.initialise(ssthInitialisationAndText2);

	  String input = "I want to create a web application. But it did not work and I have the process";

	  System.out.println(classifier.computeSentimentScores(input));

	  try (Connection dbConnection = connectToDB();
			ResultSet rs = executeQuery(dbConnection,
				  "select \"tokenizedSentence\" from question_preprocess limit 100");) {
		 System.out.println("Finish fetching query\nStart processing");
		 if (rs != null) {
			while (rs.next()) {
			   System.out.println(rs.getString(1));
			   System.out.println(classifier.computeSentimentScores(rs
					 .getString(1)));
			   System.out.println(classifier2.computeSentimentScores(rs
					 .getString(1)));
			}

		 }
	  } catch (SQLException e) {
		 e.printStackTrace();
		 e.getNextException().printStackTrace();
	  }
	  System.out.println("Done :D");
	  String pattern = "I'm missing a parenthesis. But where? :(";
	  System.out.println(classifier.computeSentimentScores(pattern));
	  System.out.println(classifier2.computeSentimentScores(pattern));
   }
}
