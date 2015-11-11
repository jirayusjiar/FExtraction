package logic;

import java.io.IOException;
import java.util.List;

import org.apache.commons.codec.binary.Base64;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.junit.Test;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import entity.ParsedTextEntity;

public class FExtractionTest {

   @Test
   public void tryLoop() throws IOException {
	  System.out.println("Try get html");
	  Document doc = Jsoup.connect("http://localhost:8080").get();
	  String[] fetchedText = doc.text().split(" ");
	  Gson gson = new Gson();
	  byte[] decodedBytes = Base64.decodeBase64(fetchedText[2]);
	  String decodedString = new String(decodedBytes);
	  System.out.println("decodedBytes " + decodedString);
	  List<ParsedTextEntity> outputFromDecode = gson.fromJson(decodedString,
			new TypeToken<List<ParsedTextEntity>>() {
			}.getType());

	  System.out.println(doc.text());
   }

}
