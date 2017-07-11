package de.ingef.eva.dataprocessor;

import static org.junit.Assert.*;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.Test;

import de.ingef.eva.data.DataSet;
import de.ingef.eva.data.DataTable;
import de.ingef.eva.data.RowElement;
import de.ingef.eva.data.SimpleRowElement;
import de.ingef.eva.data.TeradataColumnType;
import de.ingef.eva.datasource.inmemory.InMemoryDataTable;
import de.ingef.eva.error.DataTableOperationException;

public class DataTableMergeProcessorTest {

	@Test
	public void testProcess() throws DataTableOperationException {
		List<DataTable> subsets = createSubsets();
		DataTable dataTables = new DataSet("dummyData", subsets, false);
		DataTable result = new DataTableMergeProcessor("src/test/resources/").process(dataTables);
				
		assertNotNull("DataTable is null.", result);
		assertEquals("dummyData", result.getName());
		assertTrue(result.open());
		assertTrue(result.hasMoreRows());
		List<RowElement> row = result.getNextRow(true);
		assertNotNull("Row is null.", row);
		assertEquals(2, row.size());
		
		assertEquals("1", row.get(0).getContent());
		assertEquals("10.45", row.get(1).getContent());
		row = result.getNextRow(true);
		assertEquals("2", row.get(0).getContent());
		assertEquals("5.43", row.get(1).getContent());
		
		assertFalse(result.hasMoreRows());
		
		result.close();
	}
	
	@After
	public void removeTmpFile() {
		File tmpFile = new File("src/test/resources/dummyData.csv");
		tmpFile.delete();
	}
	
	private List<DataTable> createSubsets() {
		List<DataTable> subsets = new ArrayList<>(2);
		List<List<RowElement>> rows = new ArrayList<>(2);
		rows.add(Arrays.asList(
			new SimpleRowElement("pid", 0, TeradataColumnType.CHARACTER, "1"),
			new SimpleRowElement("kosten", 1, TeradataColumnType.FLOAT, "10.45")
		));
		List<RowElement> header = Arrays.asList(
			new SimpleRowElement("pid", 0, TeradataColumnType.CHARACTER, "pid"),
			new SimpleRowElement("kosten", 1, TeradataColumnType.FLOAT, "kosten")
		);
		
		subsets.add(new InMemoryDataTable("dummyData.2010", rows.listIterator(), header));
		List<List<RowElement>> rows2 = new ArrayList<>(2);
		rows2.add(Arrays.asList(
				new SimpleRowElement("pid", 0, TeradataColumnType.CHARACTER, "2"),
				new SimpleRowElement("kosten", 1, TeradataColumnType.FLOAT, "5.43")
		));
		subsets.add(new InMemoryDataTable("dummyData.2011", rows2.listIterator(), header));
		
		return subsets;
	}
}
