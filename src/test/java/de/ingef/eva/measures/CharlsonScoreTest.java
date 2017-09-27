package de.ingef.eva.measures;

import static org.junit.Assert.*;

import java.util.Collection;
import java.util.Iterator;

import org.junit.Test;

import de.ingef.eva.measures.CharlsonScore;

public class CharlsonScoreTest {
	
	@Test
	public void noPaddingBeyondLastEntry() {
		CharlsonScore ms = new CharlsonScore();

		ms.updateWeightOrAddEntry(1, 2010, "a", 2);

		ms.setYearLimits(2010, 2011);
		Collection<String> results = ms.calculateSlidingScore();
		assertEquals(1,results.size());
		
		Iterator<String> iter = results.iterator(); 
		String result = iter.next();
		//q1 2010
		assertEquals("20100101;20101231;2", result);
	}
	
	@Test
	public void paddingValuesAccountForContainedEntriesWithScore() {
		CharlsonScore ms = new CharlsonScore();

		ms.updateWeightOrAddEntry(1, 2010, "a", 2);
		ms.updateWeightOrAddEntry(1, 2010, "a", 1);
		ms.updateWeightOrAddEntry(1, 2010, "b", 2);
		ms.updateWeightOrAddEntry(2, 2010, "a", 2);
		ms.updateWeightOrAddEntry(2, 2010, "a", 1);
		ms.updateWeightOrAddEntry(2, 2010, "b", 2);

		ms.updateWeightOrAddEntry(1, 2011, "b", 2);
		ms.setYearLimits(2010, 2011);
		Collection<String> results = ms.calculateSlidingScore();
		assertEquals(5,results.size());
		
		Iterator<String> iter = results.iterator(); 
		String result = iter.next();
		//q1 2010
		assertEquals("20100101;20101231;4", result);
		
		//q2 2010
		result = iter.next();
		assertEquals("20100401;20110331;4", result);
		
		//q3 2010
		result = iter.next();
		assertEquals("20100701;20110630;2", result);
		
		//q4 2010
		result = iter.next();
		assertEquals("20101001;20110930;2", result);
		
		//q1 2011
		result = iter.next();
		assertEquals("20110101;20111231;2", result);
	}
	
	@Test
	public void stopAtQuartersInFuture() {
		CharlsonScore ms = new CharlsonScore();

		ms.updateWeightOrAddEntry(2, 2017, "b", 2);
		ms.setYearLimits(2017, 2017);
		Collection<String> results = ms.calculateSlidingScore();
		assertEquals(0,results.size());
	}

}
