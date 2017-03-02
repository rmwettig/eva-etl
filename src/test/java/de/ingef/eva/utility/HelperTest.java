package de.ingef.eva.utility;

import static org.junit.Assert.*;

import org.junit.Test;

import de.ingef.eva.configuration.DatabaseQueryConfiguration;

public class HelperTest {

	@Test
	public void testEndyearLargerThanStartyear() {
		DatabaseQueryConfiguration config = new DatabaseQueryConfiguration(2010, 2017, null);
		int[] years = Helper.extractYears(config);
		assertEquals(8, years.length);
		for(int i = 0; i < years.length; i++)
			assertEquals(2010 + i, years[i]);
	}

}
