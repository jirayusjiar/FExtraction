package logic;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Queue;

import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.IndexedWord;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.trees.semgraph.SemanticGraph;
import edu.stanford.nlp.trees.semgraph.SemanticGraphCoreAnnotations.CollapsedCCProcessedDependenciesAnnotation;
import edu.stanford.nlp.util.CoreMap;

public class ExtractionExecutor implements Runnable {

   private Queue<Integer> idQueue;
   private Queue<String> bodyQueue;
   private StanfordCoreNLP pipeline;
   private int threadNumber;
   private int numIteration;

   public ExtractionExecutor() {
   }

   public ExtractionExecutor(int inputNumIteration, int inputThreadNumber,
		 Queue<Integer> inputIdQueue, Queue<String> inputBodyQueue,
		 StanfordCoreNLP inputPipeline) {
	  this.numIteration = inputNumIteration;
	  this.threadNumber = inputThreadNumber;
	  this.idQueue = inputIdQueue;
	  this.bodyQueue = inputBodyQueue;
	  pipeline = inputPipeline;

   }

   @Override
   public void run() {
	  // parse dependency
	  System.out.println("Iteration " + numIteration + " Thread "
			+ threadNumber + " : n(Queue) = " + idQueue.size());
	  int appendingQuery = 0;
	  int localId;
	  String localBody;
	  try (Connection dbConnection = connectToDB();
			PreparedStatement preparedStatement = dbConnection
				  .prepareStatement("UPDATE question_preprocess SET \"dependencyParsed\" = ? WHERE id = ?");) {
		 dbConnection.setAutoCommit(true);
		 while (true) {
			if (!this.idQueue.isEmpty()) {

			   // Instance is appended from main thread
			   localId = this.idQueue.remove();
			   localBody = this.bodyQueue.remove();

			   String[] sentences = localBody.split("\n");
			   StringBuilder tmpBuilder = new StringBuilder();
			   for (int i = 0; i < sentences.length; ++i) {
				  tmpBuilder.append(dependencyParse(sentences[i]));
				  if (i + 1 != sentences.length)
					 tmpBuilder.append("\n");
			   }
			   localBody = tmpBuilder.toString();
			   preparedStatement.setString(1, localBody);
			   preparedStatement.setInt(2, localId);
			   preparedStatement.addBatch();
			   ++appendingQuery;
			   if (appendingQuery == 20) {
				  appendingQuery = 0;
				  preparedStatement.executeBatch();
				  preparedStatement.clearBatch();
				  System.out
						.println("Iteration "
							  + numIteration
							  + " Thread "
							  + threadNumber
							  + " : 20 queries are appending -> Execute the update batch");
			   }
			} else {
			   break;
			}
		 }

		 // Exit from the loop -> Execute update batch
		 System.out
			   .println("Iteration "
					 + numIteration
					 + " Thread "
					 + threadNumber
					 + " : Finish preparing update batch -> Execute the update batch");
		 preparedStatement.executeBatch();
		 System.out.println("Iteration " + numIteration + " Thread "
			   + threadNumber + " : Thread execution DONE :3");
	  } catch (SQLException e) {
		 e.printStackTrace();
		 e.getNextException().printStackTrace();
	  }
   }

   private Connection connectToDB() {

	  System.out.println("Iteration " + numIteration + " Thread "
			+ threadNumber + " -------- PostgreSQL "
			+ "JDBC Connection Testing ------------");

	  try {

		 Class.forName("org.postgresql.Driver");

	  } catch (ClassNotFoundException e) {

		 System.out.println("Iteration " + numIteration + " Thread "
			   + threadNumber + " Where is your PostgreSQL JDBC Driver? "
			   + "Include in your library path!");
		 e.printStackTrace();
		 return null;

	  }

	  System.out.println("Iteration " + numIteration + " Thread "
			+ threadNumber + " PostgreSQL JDBC Driver Registered!");

	  Connection connection = null;

	  try {

		 connection = DriverManager.getConnection(
			   "jdbc:postgresql://163.221.172.217:5432/so_msr2015", "oss",
			   "kenjiro+");

	  } catch (SQLException e) {

		 System.out.println("Iteration " + numIteration + " Thread "
			   + threadNumber + " Connection Failed! Check output console");
		 e.printStackTrace();
		 return null;

	  }

	  if (connection != null) {
		 System.out
			   .println("Iteration " + numIteration + " Thread " + threadNumber
					 + " You made it, take control your database now!");
		 return connection;
	  } else {
		 System.out.println("Iteration " + numIteration + " Thread "
			   + threadNumber + " Failed to make connection!");
		 return null;
	  }

   }

   private String dependencyParse(String inputText) {

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
