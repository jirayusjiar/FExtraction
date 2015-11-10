package entity;

public class ParsedTextEntity {
   public String parseText;
   public int id = 0;

   public ParsedTextEntity(int inputId, String inputText) {
	  this.parseText = inputText;
	  this.id = inputId;
   }

   public boolean equal(ParsedTextEntity input) {
	  if (input == null || input.parseText == null)
		 return false;
	  if (id == input.id && parseText.equals(input.parseText))
		 return true;
	  return false;
   }
}
