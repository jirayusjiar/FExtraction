package entity;

public class VoteInstance {
   public int[] VoteType;
   public int value;

   public VoteInstance(int inputVoteType, int inputValue) {
	  this.VoteType = new int[16];
	  this.value = 0;
	  add(inputVoteType, inputValue);
   }

   public void add(int inputVoteType, int inputValue) {
	  ++this.VoteType[inputVoteType - 1];
	  this.value += inputValue;
   }

   public void add(VoteInstance inputVoteInstance) {
	  for (int x = 0; x < 16; ++x) {
		 this.VoteType[x] += inputVoteInstance.VoteType[x];
	  }
	  this.value += inputVoteInstance.value;
   }
}
