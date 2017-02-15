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
		//mixed whitespaces and duplicated control sequences
		String[] row3 = new String[]{
				"  \nkey3 \n \n  ", "\r  value3  \n"	
			};
		r.add(row1);
		r.add(row2);
		r.add(row3);
		Collection<String[]> cleaned = p.ProcessResults(r);
		
		//number of input must not change
		assertEquals(3, cleaned.size());
		
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
			if(i==2)//row3
			{
				//number of columns must not change
				assertEquals(2, result.length);
				//even mixed characters must be removed
				//and potential whitespaces dropped
				assertEquals("key3", result[0]);
				assertEquals("value3", result[1]);
			}
			i++;
		}
	}
}
