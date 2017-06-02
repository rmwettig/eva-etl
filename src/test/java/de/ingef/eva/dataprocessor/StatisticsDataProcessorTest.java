package de.ingef.eva.dataprocessor;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import de.ingef.eva.datasource.DataTable;
import de.ingef.eva.datasource.inmemory.InMemoryDataTable;
import de.ingef.eva.error.DataTableOperationException;

public class StatisticsDataProcessorTest {

	@Test
	public void testProcess() throws DataTableOperationException {
		List<String> header = new ArrayList<>();
		header.add("Bezugsjahr");
		header.add("Quartal");
		header.add("Count");
		List<String[]> counts = new ArrayList<>();
		counts.add(new String[]{"2008", "1", "1000"});
		counts.add(new String[]{"2008", "2", "500"});
		counts.add(new String[]{"2008", "3", "2000"});
		counts.add(new String[]{"2008", "4", "3000"});
		counts.add(new String[]{"2009", "1", "2000"});
		counts.add(new String[]{"2009", "2", "4000"});
		counts.add(new String[]{"2009", "3", "4000"});
		counts.add(new String[]{"2009", "4", "3000"});
		DataTable himi = new InMemoryDataTable("himi", counts.listIterator(), header);
		DataTable hemi = new InMemoryDataTable("hemi", counts.listIterator(), header);
		
		DataTable result = new StatisticsDataProcessor().process(himi, hemi);
		
		/*
		 * Expected result:
		 * 2008Q1 1000       1000
		 * 2008Q2  500        500
		 * 2008Q3 2000       2000
		 * 2008Q4 3000       3000
		 * 2009Q1 2000 (2.0) 2000 (2.0)
		 * 2009Q2 4000 (8.0) 4000 (8.0)
		 * 2009Q3 4000 (2.0) 4000 (2.0)
		 * 2009Q4 3000 (1.0) 3000 (1.0)  
		 */
		String[] row = result.getNextRow();
		assertNotNull(row);
		assertEquals(3, row.length);
		
		assertEquals("2008Q1", row[0]);
		assertEquals("1000", row[1]);
		assertEquals("1000", row[2]);
		
		row = result.getNextRow();
		assertEquals("2008Q2", row[0]);
		assertEquals("500", row[1]);
		assertEquals("500", row[2]);
		
		row = result.getNextRow();
		assertEquals("2008Q3", row[0]);
		assertEquals("2000", row[1]);
		assertEquals("2000", row[2]);
		
		row = result.getNextRow();
		assertEquals("2008Q4", row[0]);
		assertEquals("3000", row[1]);
		assertEquals("3000", row[2]);
		
		row = result.getNextRow();
		assertEquals("2009Q1", row[0]);
		assertEquals("2000 (2.0)", row[1]);
		assertEquals("2000 (2.0)", row[2]);
		
		row = result.getNextRow();
		assertEquals("2009Q2", row[0]);
		assertEquals("4000 (8.0)", row[1]);
		assertEquals("4000 (8.0)", row[2]);

		row = result.getNextRow();
		assertEquals("2009Q3", row[0]);
		assertEquals("4000 (2.0)", row[1]);
		assertEquals("4000 (2.0)", row[2]);

		row = result.getNextRow();
		assertEquals("2009Q4", row[0]);
		assertEquals("3000 (1.0)", row[1]);
		assertEquals("3000 (1.0)", row[2]);
	}

}
