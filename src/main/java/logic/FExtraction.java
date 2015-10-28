package logic;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.IndexedWord;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.trees.semgraph.SemanticGraph;
import edu.stanford.nlp.trees.semgraph.SemanticGraphCoreAnnotations.CollapsedCCProcessedDependenciesAnnotation;
import edu.stanford.nlp.util.CoreMap;

public class FExtraction {
   // Last index is around 26 million == querySize*numIteration>26000000

   // Test
   // private static final int querySize = 10;
   // private static final int numIteration = 2;
   // private static final int numThread = 5;

   // Real
   private static final int querySize = 1000000;
   private static final int numIteration = 27;

   private static StanfordCoreNLP pipeline;

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
	  Properties props = new Properties();
	  props.setProperty("annotators",
			"tokenize, ssplit, pos, lemma, ner, parse, dcoref");
	  pipeline = new StanfordCoreNLP(props);
	  System.out.println("Start the execution...");
	  for (int i = 0; i < numIteration; ++i)
		 execution(i, querySize);

   }

   // Divide dataset into small size
   private static void execution(int index, int querySize) {

	  System.out.println("Iteration " + (index + 1) + " Execution from id >="
			+ (index * querySize) + " and id < " + ((index + 1) * querySize));
	  try (Connection dbConnection = connectToDB();
			ResultSet rs = executeQuery(dbConnection,
				  "select id,\"tokenizedSentence\" from question_preprocess where id >="
						+ (index * querySize) + " and id < "
						+ ((index + 1) * querySize));
			PreparedStatement preparedStatement = dbConnection
				  .prepareStatement("UPDATE question_preprocess SET \"dependencyParsed\" = ? WHERE id = ?");) {

		 System.out.println("Iteration " + (index + 1)
			   + " Finish fetching query\nStart processing");
		 if (rs != null) {

			while (rs.next()) {
			   String[] sentences = rs.getString("tokenizedSentence").split(
					 "\n");
			   StringBuilder tmpBuilder = new StringBuilder();
			   for (int i = 0; i < sentences.length; ++i) {
				  tmpBuilder.append(dependencyParse(sentences[i]));
				  if (i + 1 != sentences.length)
					 tmpBuilder.append("\n");
			   }
			   preparedStatement.setString(1, tmpBuilder.toString());
			   preparedStatement.setInt(2, rs.getInt("id"));
			   preparedStatement.addBatch();
			}
			preparedStatement.executeBatch();

			System.out.println("Iteration " + (index + 1)
				  + " Finish submitting all the query");

			System.out.println("Iteration " + (index + 1)
				  + " Finish preprocessing\n");

		 }
	  } catch (SQLException e) {
		 e.printStackTrace();
		 e.getNextException().printStackTrace();
	  }
	  System.out.println("Iteration " + (index + 1)
			+ " Iteration execution DONE :D");
   }

   private static String dependencyParse(String inputText) {

	  StringBuilder output = new StringBuilder();

	  // creates a StanfordCoreNLP object, with POS tagging, lemmatization, NER,
	  // parsing, and coreference resolution

	  // create an empty Annotation just with the given text
	  Annotation document = new Annotation(inputText);

	  // run all Annotators on this text
	  pipeline.annotate(document);

	  // these are all the sentences in this document
	  // a CoreMap is essentially a Map that uses class objects as keys and has
	  // values with custom types
	  List<CoreMap> sentences = document.get(SentencesAnnotation.class);

	  for (CoreMap sentence : sentences) {

		 // this is the Stanford dependency graph of the current sentence
		 SemanticGraph dependencies = sentence
			   .get(CollapsedCCProcessedDependenciesAnnotation.class);

		 if (!dependencies.getRoots().isEmpty()) {
			IndexedWord root = dependencies.getFirstRoot();
			output.append(String.format("root(ROOT-0, %s-%d)%n", root.word(),
				  root.index()));
		 }
		 output.append(dependencies.toList());
	  }
	  return output.toString().replace("\n", "|");
   }
}
