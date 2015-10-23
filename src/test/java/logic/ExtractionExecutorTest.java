package logic;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.junit.Test;

public class ExtractionExecutorTest {

   @Test
   public void test() {
	  ExtractionExecutor testExt = new ExtractionExecutor();
	  String input = " <p><code><frameset rows=\"36, 95%\" border=\"0\">    <frame src=\"alfa.html\" noresize scrolling=\"no\">    <frame src=\"http://translate.google.com/translate?js=y&prev=_t&hl=en&ie=UTF-8&layout=1&eotf=0&u=http://www.apple.com/&sl=en&tl=zh-CN\"></frameset></code></p><p>How to hide the google top frame? JS? Jquery? And how...</p><p>Thanks!</p>";
	  Document docEnt = Jsoup.parse(input.trim());
	  System.out.println(testExt.recursiveExtraction(docEnt.head())
			+ testExt.recursiveExtraction(docEnt.body()));
   }
}
