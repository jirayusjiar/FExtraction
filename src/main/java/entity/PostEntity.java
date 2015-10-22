package entity;

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.commons.lang3.StringEscapeUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

public class PostEntity {
    public int id;
    public int postTypeId;
    public int parentId;
    public int acceptedAnswerId;
    public Date creationDate;
    public int score;
    public int viewCount;
    public String body;
    public int ownerUserId;
    public String ownerDisplayName;
    public int lastEditorUserId;
    public String lastEditorUserName;
    public Date lastEditDate;
    public Date lastActivityDate;
    public Date communityOwnedDate;
    public Date closedDate;
    public String title;
    public String tags;
    public int answerCount;
    public int commentCount;
    public int favoriteCount;

    public PostEntity(ResultSet inputResultSet) throws SQLException {
	  id = inputResultSet.getInt(1);
	  postTypeId = inputResultSet.getInt(2);
	  parentId = inputResultSet.getInt(3);
	  acceptedAnswerId = inputResultSet.getInt(4);
	  creationDate = inputResultSet.getDate(5);
	  score = inputResultSet.getInt(6);
	  viewCount = inputResultSet.getInt(7);
	  body = cleanText(inputResultSet.getString(8));
	  ownerUserId = inputResultSet.getInt(9);
	  ownerDisplayName = cleanText(inputResultSet.getString(10));
	  lastEditorUserId = inputResultSet.getInt(11);
	  lastEditorUserName = cleanText(inputResultSet.getString(12));
	  lastEditDate = inputResultSet.getDate(13);
	  lastActivityDate = inputResultSet.getDate(14);
	  communityOwnedDate = inputResultSet.getDate(15);
	  closedDate = inputResultSet.getDate(16);
	  title = cleanText(inputResultSet.getString(17));
	  tags = cleanText(inputResultSet.getString(18));
	  answerCount = inputResultSet.getInt(19);
	  commentCount = inputResultSet.getInt(20);
	  favoriteCount = inputResultSet.getInt(21);
    }

    private static String cleanText(String inputString) {
	  if (inputString == null || inputString.isEmpty())
		return inputString;
	  String htmlText = StringEscapeUtils.unescapeHtml4(inputString);
	  String rawText = getTextWithoutCode(Jsoup.parse(htmlText)).trim();
	  return rawText;

    }

    private static String getTextWithoutCode(Document inputHtmlDocument) {
	  return recursiveExtraction(inputHtmlDocument.head())
		    + recursiveExtraction(inputHtmlDocument.body());
    }

    private static String recursiveExtraction(Element inputElement) {
	  StringBuilder output = new StringBuilder("");
	  if (!inputElement.tagName().equals("code")
		    && !inputElement.ownText().isEmpty())
		output = new StringBuilder(inputElement.ownText() + " ");
	  for (Element child : inputElement.children())
		output.append(recursiveExtraction(child) + " ");
	  return output.toString();
    }

}
