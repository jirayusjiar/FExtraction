package entity;

public class ParsedTextEntity {
   public String parseText;
   public int id = 0;

   public ParsedTextEntity(int inputId, String inputText) {
	  this.parseText = inputText;
	  this.id = inputId;
   }
}
