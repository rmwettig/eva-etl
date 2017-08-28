package de.ingef.eva.dataprocessor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import de.ingef.eva.data.DataTable;
import de.ingef.eva.data.RowElement;
import de.ingef.eva.data.SimpleRowElement;
import de.ingef.eva.data.TeradataColumnType;
import de.ingef.eva.datasource.inmemory.InMemoryDataTable;
import de.ingef.eva.error.DataTableOperationException;

public class StatisticsDataProcessorTest {

	@Test
	public void testProcess() throws DataTableOperationException {
		List<RowElement> header = new ArrayList<>();
		header.add(new SimpleRowElement("Bezugsjahr", 0, TeradataColumnType.INTEGER, "Bezugsjahr"));
		header.add(new SimpleRowElement("Quartal", 1, TeradataColumnType.INTEGER, "Quartal"));
		header.add(new SimpleRowElement("Count", 2, TeradataColumnType.INTEGER, "Count"));

		List<List<RowElement>> counts = createCountTable();
		
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
		List<RowElement> row = result.getNextRow(true);
		assertNotNull(row);
		assertEquals(3, row.size());
		
		assertEquals("2008Q1", row.get(0).getContent());
		assertEquals("1.000", row.get(1).getContent());
		assertEquals("1.000", row.get(2).getContent());
		
		row = result.getNextRow(true);
		assertEquals("2008Q2", row.get(0).getContent());
		assertEquals("500", row.get(1).getContent());
		assertEquals("500", row.get(2).getContent());
		
		row = result.getNextRow(true);
		assertEquals("2008Q3", row.get(0).getContent());
		assertEquals("2.000", row.get(1).getContent());
		assertEquals("2.000", row.get(2).getContent());
		
		row = result.getNextRow(true);
		assertEquals("2008Q4", row.get(0).getContent());
		assertEquals("3.000", row.get(1).getContent());
		assertEquals("3.000", row.get(2).getContent());
		
		row = result.getNextRow(true);
		assertEquals("2009Q1", row.get(0).getContent());
		assertEquals("2.000 (2,0)", row.get(1).getContent());
		assertEquals("2.000 (2,0)", row.get(2).getContent());
		
		row = result.getNextRow(true);
		assertEquals("2009Q2", row.get(0).getContent());
		assertEquals("4.000 (8,0)", row.get(1).getContent());
		assertEquals("4.000 (8,0)", row.get(2).getContent());

		row = result.getNextRow(true);
		assertEquals("2009Q3", row.get(0).getContent());
		assertEquals("4.000 (2,0)", row.get(1).getContent());
		assertEquals("4.000 (2,0)", row.get(2).getContent());

		row = result.getNextRow(true);
		assertEquals("2009Q4", row.get(0).getContent());
		assertEquals("3.000 (1,0)", row.get(1).getContent());
		assertEquals("3.000 (1,0)", row.get(2).getContent());
	}

	private List<List<RowElement>> createCountTable() {
		List<List<RowElement>> counts = new ArrayList<>();
		List<RowElement> row = new ArrayList<>(3);
		row.add(new SimpleRowElement("Bezugsjahr", 0, TeradataColumnType.INTEGER, "2008"));
		row.add(new SimpleRowElement("Quartal", 1, TeradataColumnType.INTEGER, "1"));
		row.add(new SimpleRowElement("Count", 2, TeradataColumnType.INTEGER, "1000"));
		counts.add(row);
		
		List<RowElement> row2 = new ArrayList<>(3);
		row2.add(new SimpleRowElement("Bezugsjahr", 0, TeradataColumnType.INTEGER, "2008"));
		row2.add(new SimpleRowElement("Quartal", 1, TeradataColumnType.INTEGER, "2"));
		row2.add(new SimpleRowElement("Count", 2, TeradataColumnType.INTEGER, "500"));
		counts.add(row2);
		
		List<RowElement> row3 = new ArrayList<>(3);
		row3.add(new SimpleRowElement("Bezugsjahr", 0, TeradataColumnType.INTEGER, "2008"));
		row3.add(new SimpleRowElement("Quartal", 1, TeradataColumnType.INTEGER, "3"));
		row3.add(new SimpleRowElement("Count", 2, TeradataColumnType.INTEGER, "2000"));
		counts.add(row3);
		
		List<RowElement> row4 = new ArrayList<>(3);
		row4.add(new SimpleRowElement("Bezugsjahr", 0, TeradataColumnType.INTEGER, "2008"));
		row4.add(new SimpleRowElement("Quartal", 1, TeradataColumnType.INTEGER, "4"));
		row4.add(new SimpleRowElement("Count", 2, TeradataColumnType.INTEGER, "3000"));
		counts.add(row4);

		List<RowElement> row5 = new ArrayList<>(3);
		row5.add(new SimpleRowElement("Bezugsjahr", 0, TeradataColumnType.INTEGER, "2009"));
		row5.add(new SimpleRowElement("Quartal", 1, TeradataColumnType.INTEGER, "1"));
		row5.add(new SimpleRowElement("Count", 2, TeradataColumnType.INTEGER, "2000"));
		counts.add(row5);
		
		List<RowElement> row6 = new ArrayList<>(3);
		row6.add(new SimpleRowElement("Bezugsjahr", 0, TeradataColumnType.INTEGER, "2009"));
		row6.add(new SimpleRowElement("Quartal", 1, TeradataColumnType.INTEGER, "2"));
		row6.add(new SimpleRowElement("Count", 2, TeradataColumnType.INTEGER, "4000"));
		counts.add(row6);
		
		List<RowElement> row8 = new ArrayList<>(3);
		row8.add(new SimpleRowElement("Bezugsjahr", 0, TeradataColumnType.INTEGER, "2009"));
		row8.add(new SimpleRowElement("Quartal", 1, TeradataColumnType.INTEGER, "3"));
		row8.add(new SimpleRowElement("Count", 2, TeradataColumnType.INTEGER, "4000"));
		counts.add(row8);
		
		List<RowElement> row9 = new ArrayList<>(3);
		row9.add(new SimpleRowElement("Bezugsjahr", 0, TeradataColumnType.INTEGER, "2009"));
		row9.add(new SimpleRowElement("Quartal", 1, TeradataColumnType.INTEGER, "4"));
		row9.add(new SimpleRowElement("Count", 2, TeradataColumnType.INTEGER, "3000"));
		counts.add(row9);
		return counts;
	}

}
