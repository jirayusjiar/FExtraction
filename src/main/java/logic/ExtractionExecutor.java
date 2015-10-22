package logic;

import ipeirotis.readability.Readability;

import java.util.concurrent.Callable;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import entity.PostEntity;
import entity.ReadabilityEntity;

public class ExtractionExecutor implements Callable {

   private String body;
   private String tags;
   private ReadabilityEntity output;

   public ExtractionExecutor(PostEntity inputData) {
	  output = new ReadabilityEntity();
	  output.id = inputData.id;
	  this.body = inputData.body;
	  this.tags = inputData.tags;
	  output.hasAnswer = (inputData.acceptedAnswerId != 0);
   }

   public ReadabilityEntity call() throws Exception {

	  output.hasCode = this.body.contains("<code>");

	  // Count number of tag
	  for (int x = 0; x < this.tags.length(); ++x)
		 if (this.tags.charAt(x) == '<')
			++output.nTag;

	  // Get rawText of post
	  String rawText = getTextWithoutCode(Jsoup.parse(this.body)).trim();

	  // Compute contentLength & readability scores
	  Readability readabilitEntity = new Readability(rawText);
	  output.contentLength = readabilitEntity.getCharacters();
	  output.colemanIndex = readabilitEntity.getColemanLiau();
	  output.fleschKincaid = readabilitEntity.getFleschKincaidGradeLevel();
	  output.fleschReading = readabilitEntity.getFleschReadingEase();
	  output.gunningFox = readabilitEntity.getGunningFog();
	  output.smog = readabilitEntity.getSMOG();
	  output.sentenceCount = readabilitEntity.getSentences();
	  output.wordCount = readabilitEntity.getWords();

	  return output;
   }

   private String getTextWithoutCode(Document inputHtmlDocument) {
	  return recursiveExtraction(inputHtmlDocument.head())
			+ recursiveExtraction(inputHtmlDocument.body());
   }

   private String recursiveExtraction(Element inputElement) {
	  StringBuilder output = new StringBuilder("");
	  if (!inputElement.tagName().equals("code")
			&& !inputElement.ownText().isEmpty()) {
		 output = new StringBuilder(inputElement.ownText() + " ");
	  } else {
		 // <code> section
		 this.output.loc += inputElement.ownText().split("\n").length;
	  }

	  for (Element child : inputElement.children())
		 output.append(recursiveExtraction(child) + " ");
	  return output.toString();
   }
}
