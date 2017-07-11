package de.ingef.eva.measures;

import static org.junit.Assert.*;

import java.util.Collection;
import java.util.Iterator;

import org.junit.Test;

import de.ingef.eva.measures.CharlsonScore;

public class CharlsonScoreTest {

	@Test
	public void testIncompleteData() {
		CharlsonScore ms = new CharlsonScore();
		/*
		 * internal representation:
		 * 	4 entries: 2 for 2010Q1 and 2010Q2 for class a
		 *  2 entries: 1 for 2010Q1 and 2010Q2 for class b
		 */
		ms.updateWeightOrAddEntry(1, 2010, "a", 2);
		ms.updateWeightOrAddEntry(1, 2010, "a", 1);
		ms.updateWeightOrAddEntry(1, 2010, "b", 2);
		ms.updateWeightOrAddEntry(2, 2010, "a", 2);
		ms.updateWeightOrAddEntry(2, 2010, "a", 1);
		ms.updateWeightOrAddEntry(2, 2010, "b", 2);
		//should not change score
		ms.updateWeightOrAddEntry(1, 2011, "b", 3);
		
		Collection<String> results = ms.calculateSlidingScore();
		assertEquals(3,results.size());
		Iterator<String> iter = results.iterator(); 
		
		String result = iter.next();
		assertEquals("20100101;20101231;4", result);
		
		result = iter.next();
		assertEquals("20100401;20110331;5", result);
		
		result = iter.next();
		assertEquals("20110101;20111231;3", result);
	}
	
	@Test
	public void testYearToYear() {
		CharlsonScore ms = new CharlsonScore();
		
		ms.updateWeightOrAddEntry(1, 2010, "a", 2);
		ms.updateWeightOrAddEntry(1, 2010, "a", 1);
		ms.updateWeightOrAddEntry(1, 2010, "b", 2);
		ms.updateWeightOrAddEntry(2, 2010, "a", 2);
		ms.updateWeightOrAddEntry(2, 2010, "a", 1);
		ms.updateWeightOrAddEntry(2, 2010, "b", 2);
		ms.updateWeightOrAddEntry(3, 2010, "a", 2);
		ms.updateWeightOrAddEntry(3, 2010, "a", 1);
		ms.updateWeightOrAddEntry(3, 2010, "b", 2);
		ms.updateWeightOrAddEntry(4, 2010, "a", 2);
		ms.updateWeightOrAddEntry(4, 2010, "a", 1);
		ms.updateWeightOrAddEntry(4, 2010, "b", 2);
		//should be ignored
		ms.updateWeightOrAddEntry(1, 2011, "b", 3);
		Collection<String> results = ms.calculateSlidingScore();
		assertEquals(5,results.size());
		
		Iterator<String> iter = results.iterator(); 
		String result = iter.next();
		//start Q1, end Q4
		assertEquals("20100101;20101231;4", result);
		
		result = iter.next();
		//start Q2 y, end Q1 y+1
		assertEquals("20100401;20110331;5", result);
		
		result = iter.next();
		//start Q3 y, end Q2 y+1
		assertEquals("20100701;20110630;5", result);
		
		result = iter.next();
		//start Q4 y, end Q3 y+1
		assertEquals("20101001;20110930;5", result);
		
		result = iter.next();
		//start Q1, end Q4
		assertEquals("20110101;20111231;3", result);
	}
	
	@Test
	public void testDoNotTakeMoreThanFourQuarters() {
		CharlsonScore ms = new CharlsonScore();

		ms.updateWeightOrAddEntry(1, 2010, "a", 2);
		ms.updateWeightOrAddEntry(1, 2010, "a", 1);
		ms.updateWeightOrAddEntry(1, 2010, "b", 2);
		ms.updateWeightOrAddEntry(2, 2010, "a", 2);
		ms.updateWeightOrAddEntry(2, 2010, "a", 1);
		ms.updateWeightOrAddEntry(2, 2010, "b", 2);
		//should be ignored
		ms.updateWeightOrAddEntry(1, 2011, "b", 2);
		Collection<String> results = ms.calculateSlidingScore();
		assertEquals(3,results.size());
		
		Iterator<String> iter = results.iterator(); 
		String result = iter.next();
		assertEquals("20100101;20101231;4", result);

		result = iter.next();
		assertEquals("20100401;20110331;4", result);
		
		result = iter.next();
		assertEquals("20110101;20111231;2", result);
	}

}
