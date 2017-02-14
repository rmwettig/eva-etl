package de.ingef.eva.processor;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Collection;

import org.junit.Test;

public class CleanRowsResultProcessorTest {

	
	
	@Test
	public void testRemovalOfControlSequencesAndWhitespaces() {
		CleanRowsResultProcessor p = new CleanRowsResultProcessor();
		
		Collection<String[]> r = new ArrayList<String[]>(2);
		//has control sequences
		String[] row1 = new String[]{
			"ke\r\ny1", "val\rue1"	
		};
		//has leading and trailing whitespaces
		String[] row2 = new String[]{
			"  key2", "  value2  "	
		};
		r.add(row1);
		r.add(row2);
		Collection<String[]> cleaned = p.ProcessResults(r);
		
		//number of input must not change
		assertEquals(2, cleaned.size());
		
		int i = 0;
		for(String[] result : cleaned)
		{
			if(i==0) //row1
			{
				//number of columns must not change
				assertEquals(2, result.length);
				//control sequences removed
				assertEquals("key1", result[0]);
				assertEquals("value1", result[1]);
			}
			if(i==1)//row2
			{
				//number of columns must not change
				assertEquals(2, result.length);
				//whitespaces removed
				assertEquals("key2", result[0]);
				assertEquals("value2", result[1]);
			}
			i++;
		}
	}
}
