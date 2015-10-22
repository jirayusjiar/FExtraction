package entity;

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.commons.lang3.StringEscapeUtils;

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
	  id = inputResultSet.getInt("id");
	  postTypeId = inputResultSet.getInt("post_type_id");
	  parentId = inputResultSet.getInt("parent_id");
	  acceptedAnswerId = inputResultSet.getInt("accepted_answer_id");
	  creationDate = inputResultSet.getDate("creation_date");
	  score = inputResultSet.getInt("score");
	  viewCount = inputResultSet.getInt("view_count");
	  body = cleanText(inputResultSet.getString("body"));
	  ownerUserId = inputResultSet.getInt("owner_user_id");
	  ownerDisplayName = cleanText(inputResultSet
			.getString("owner_display_name"));
	  lastEditorUserId = inputResultSet.getInt("last_editor_user_id");
	  lastEditorUserName = cleanText(inputResultSet
			.getString("last_editor_display_name"));
	  lastEditDate = inputResultSet.getDate("last_edit_date");
	  lastActivityDate = inputResultSet.getDate("last_activity_date");
	  communityOwnedDate = inputResultSet.getDate("community_owned_date");
	  closedDate = inputResultSet.getDate("closed_date");
	  title = cleanText(inputResultSet.getString("title"));
	  tags = cleanText(inputResultSet.getString("tags"));
	  answerCount = inputResultSet.getInt("answer_count");
	  commentCount = inputResultSet.getInt("comment_count");
	  favoriteCount = inputResultSet.getInt("favorite_count");
   }

   private static String cleanText(String inputString) {
	  if (inputString == null || inputString.isEmpty())
		 return inputString;
	  String htmlText = StringEscapeUtils.unescapeHtml4(inputString);
	  return htmlText.trim();

   }

}
