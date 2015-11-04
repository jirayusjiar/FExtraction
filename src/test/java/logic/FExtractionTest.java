package logic;

import org.junit.Test;

public class FExtractionTest {

   @Test
   public void tryLoop() {

	  int querySize = 10000;
	  int numIteration = 2600;
	  for (int index = 0; index < numIteration; ++index)
		 System.out.println("Iteration " + (index + 1)
			   + " Execution from id >=" + (index * querySize) + " and id < "
			   + ((index + 1) * querySize));
   }

}
