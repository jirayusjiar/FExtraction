package logic;

import java.util.concurrent.Callable;

import org.apache.commons.lang3.StringEscapeUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import entity.ParsedTextEntity;
import entity.PostEntity;

public class ExtractionExecutor implements Callable {

   private String body;
   private int id;

   public ExtractionExecutor() {
   }

   public ExtractionExecutor(PostEntity inputData) {
	  this.id = inputData.id;
	  this.body = inputData.body;
   }

   public ParsedTextEntity call() {
	  return new ParsedTextEntity(this.id,
			getTextWithoutCode(Jsoup.parse(cleanText(this.body))));
   }

   protected String getTextWithoutCode(Document inputHtmlDocument) {
	  return (recursiveExtraction(inputHtmlDocument.head()) + recursiveExtraction(inputHtmlDocument
			.body())).replace("\n", " ").trim().replace("\\x00", "");
   }

   protected String recursiveExtraction(Element inputElement) {
	  if (inputElement == null)
		 return "";
	  StringBuilder output = new StringBuilder("");
	  if (inputElement.tagName() == null)
		 return "";
	  else if (!inputElement.tagName().equals("code")) {
		 // Note <code> section
		 if (!inputElement.ownText().isEmpty())
			output = new StringBuilder(inputElement.ownText() + " ");
	  }

	  for (Element child : inputElement.children())
		 output.append(recursiveExtraction(child) + " ");
	  return output.toString();
   }

   private String cleanText(String inputString) {
	  if (inputString == null || inputString.isEmpty())
		 return inputString;
	  String htmlText = StringEscapeUtils.unescapeHtml4(inputString);
	  return htmlText.trim();

   }

}
