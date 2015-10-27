package entity;

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;

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
	  body = inputResultSet.getString("body");
   }

}
