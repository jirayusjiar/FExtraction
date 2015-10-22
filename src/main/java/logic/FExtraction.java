package logic;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

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

    public static void main(String[] args) throws SQLException {

	  Connection dbConnection = connectToDB();

	  ResultSet rs = executeQuery(dbConnection,
		    "select * from posts limit 10");
	  if (rs != null) {

		while (rs.next()) {
		    System.out.printf("%d\t%s\t%s\t%d\n", rs.getInt(1),
				rs.getString(2), rs.getString(3), rs.getInt(4));
		}

	  }
    }
}
