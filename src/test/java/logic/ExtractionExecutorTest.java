package logic;

import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.Properties;

import org.junit.Before;
import org.junit.Test;

import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.IndexedWord;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.trees.semgraph.SemanticGraph;
import edu.stanford.nlp.trees.semgraph.SemanticGraphCoreAnnotations.CollapsedCCProcessedDependenciesAnnotation;
import edu.stanford.nlp.util.CoreMap;

public class ExtractionExecutorTest {

   private StanfordCoreNLP pipeline, pipeline2;

   @Before
   public void setUp() {

	  Properties props2 = new Properties();
	  props2.setProperty("annotators", "tokenize,ssplit,parse");
	  pipeline2 = new StanfordCoreNLP(props2);

	  Properties props = new Properties();
	  props.setProperty("annotators",
			"tokenize, ssplit, pos, lemma, ner, parse, dcoref");
	  pipeline = new StanfordCoreNLP(props);
   }

   @Test
   public void dependencyTest() {
	  StringBuilder output = new StringBuilder();
	  assertEquals(
			dependencyParse(pipeline,
				  "Sorry :) I dont want to hack the system!!"),
			dependencyParse(pipeline2,
				  "Sorry :) I dont want to hack the system!!"));
	  assertEquals(dependencyParse(pipeline, ":) is there another way?"),
			dependencyParse(pipeline2, ":) is there another way?"));
	  // assertEquals(dependencyParse("What are you trying to do?"));
	  // assertEquals(dependencyParse("Why can't you just store the 'Range'?"));
   }

   private String dependencyParse(StanfordCoreNLP inputPipeline,
		 String inputText) {
	  System.out.println("Parsing : " + inputText);

	  StringBuilder output = new StringBuilder();

	  // creates a StanfordCoreNLP object, with POS tagging, lemmatization, NER,
	  // parsing, and coreference resolution

	  // create an empty Annotation just with the given text
	  Annotation document = new Annotation(inputText);

	  // run all Annotators on this text
	  inputPipeline.annotate(document);

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
