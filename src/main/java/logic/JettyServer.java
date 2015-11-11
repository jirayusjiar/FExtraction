package logic;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.codec.binary.Base64;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;

import com.google.gson.Gson;

import entity.ParsedTextEntity;

public class JettyServer extends AbstractHandler {
   private static volatile Queue<List<ParsedTextEntity>> distributedDataList = new ArrayDeque<List<ParsedTextEntity>>();
   private static volatile List<ParsedTextEntity> tmpList = new ArrayList<ParsedTextEntity>();
   private static volatile int distributedId = 0;
   private static volatile boolean running = false;
   private static final int fetchSize = 2000;
   private static final int nIteration = 13000;
   private static int index = 0;// 4575
   private static int leftToExecute;

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

   private static void testDBConnection() throws SQLException,
		 ClassNotFoundException {

	  System.out.println("-------- PostgreSQL "
			+ "JDBC Connection Testing ------------");

	  try {

		 Class.forName("org.postgresql.Driver");

	  } catch (ClassNotFoundException e) {

		 System.out.println("Where is your PostgreSQL JDBC Driver? "
			   + "Include in your library path!");
		 e.printStackTrace();
		 throw e;

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
		 throw e;

	  }

	  if (connection != null) {
		 System.out.println("You made it, DB connection [OK]");
	  } else {
		 System.out.println("Failed to make connection!");
		 throw new SQLException("Cannot connect to the database");
	  }

   }

   private static void getCount() {
	  try (Connection dbConnection = connectToDB();
			ResultSet rs = executeQuery(dbConnection,
				  "SELECT count(*) FROM question_features where \"politeness\"=0");) {

		 if (rs != null) {
			while (rs.next()) {
			   leftToExecute = rs.getInt(1);
			}
		 }
	  } catch (SQLException e) {
		 e.printStackTrace();
		 e.getNextException().printStackTrace();
	  }
   }

   private static void getCalculate() {
	  do {
		 if (index < nIteration)
			++index;
		 else
			break;
		 try (Connection dbConnection = connectToDB();
			   ResultSet rs = executeQuery(
					 dbConnection,
					 "SELECT question_preprocess.id, \"tokenizedSentence\" FROM question_preprocess where id >="
						   + (index * fetchSize)
						   + " and id < "
						   + ((index + 1) * fetchSize)
						   + " and \"dependencyParsed\" is null");) {
			System.out.println("Iteration " + (index + 1)
				  + " Finish fetching query");
			if (rs != null) {
			   while (rs.next()) {
				  tmpList.add(new ParsedTextEntity(rs.getInt(1), rs
						.getString(2)));
				  if (tmpList.size() == 200) {
					 distributedDataList.add(tmpList);
					 tmpList = new ArrayList<ParsedTextEntity>();
				  }
			   }
			}
		 } catch (SQLException e) {
			e.printStackTrace();
			e.getNextException().printStackTrace();
		 }
		 try {
			Thread.sleep(20);
		 } catch (InterruptedException e) {
			e.printStackTrace();
		 }
	  } while (distributedDataList.size() < 20);
	  if (tmpList.size() != 0) {
		 distributedDataList.add(tmpList);
		 tmpList = new ArrayList<ParsedTextEntity>();
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

   public void handle(String target, Request baseRequest,
		 HttpServletRequest request, HttpServletResponse response)
		 throws IOException, ServletException {
	  response.setContentType("text/html;charset=utf-8");
	  response.setStatus(HttpServletResponse.SC_OK);
	  baseRequest.setHandled(true);

	  if (distributedDataList.size() != 0) {
		 List<ParsedTextEntity> output = distributedDataList.remove();

		 if (!running && distributedDataList.size() < 8) {
			(new Thread() {
			   public void run() {
				  running = true;
				  getCalculate();
				  running = false;
			   }
			}).start();
		 }

		 distributedId += output.size();
		 Gson gson = new Gson();
		 String jsonText = gson.toJson(output);
		 byte[] encodedBytes = Base64.encodeBase64(jsonText.getBytes());
		 String encodedString = new String(encodedBytes);
		 response.getWriter().print(
			   distributedId + "\n" + leftToExecute + "\n" + encodedString);
	  } else
		 response.getWriter().print(-1);
   }

   public static void main(String[] args) throws Exception {

	  testDBConnection();

	  getCount();
	  getCalculate();

	  Server server = new Server(8080);
	  server.setHandler(new JettyServer());

	  server.start();
	  server.join();
   }

}
