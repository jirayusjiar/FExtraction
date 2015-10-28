package logic;

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

   private StanfordCoreNLP pipeline;

   @Before
   public void setUp() {
	  Properties props = new Properties();
	  props.setProperty("annotators",
			"tokenize, ssplit, pos, lemma, ner, parse, dcoref");
	  pipeline = new StanfordCoreNLP(props);
   }

   @Test
   public void dependencyTest() {
	  StringBuilder output = new StringBuilder();
	  output.append(dependencyParse("Sorry :) I dont want to hack the system!!"));
	  output.append(dependencyParse(":) is there another way?"));
	  output.append(dependencyParse("What are you trying to do?"));
	  output.append(dependencyParse("Why can't you just store the 'Range'?"));
	  System.out.println(output.toString());
   }

   private String dependencyParse(String inputText) {
	  System.out.println("Parsing : " + inputText);

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
