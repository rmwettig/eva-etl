package de.ingef.eva.mapping;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;

import de.ingef.eva.configuration.Configuration;
import de.ingef.eva.data.DataTable;
import de.ingef.eva.data.RowElement;
import de.ingef.eva.data.SimpleRowElement;
import de.ingef.eva.data.TeradataColumnType;
import de.ingef.eva.error.DataTableOperationException;
import de.ingef.eva.utility.io.IOManager;


public class ProcessPidDecodeTest {

	private DataTable unwantedPids;
	private DataTable unfilteredPids;
	
	@Before
	public void setUpDataTables() {
		unwantedPids = new DataTable() {
			private String[] rows = new String[]{"0000001234", "0000005678"};
			private int i = 0;
			@Override
			public boolean open() throws DataTableOperationException {
				return i < rows.length;
			}
			
			@Override
			public boolean hasMoreRows() throws DataTableOperationException {
				return i < rows.length;
			}
			
			@Override
			public List<RowElement> getNextRow(boolean ignoreMalformedRows) throws DataTableOperationException {
				String pid = rows[i++];
				List<RowElement> converted = new ArrayList<>();
				converted.add(new SimpleRowElement("pid", 0, TeradataColumnType.CHARACTER, pid));

				return converted;
			}
			
			@Override
			public String getName() {
				return "unwantedPids";
			}
						
			@Override
			public List<RowElement> getColumnNames() throws DataTableOperationException {
				return Arrays.asList(
					new SimpleRowElement("pid", 0, TeradataColumnType.CHARACTER, "pid")
				);
			}
			
			@Override
			public void close() throws DataTableOperationException {
			}
			
			@Override
			public String getDelimiter() {
				return "";
			}
		};
		
		unfilteredPids = new DataTable(){
			private String[][] rows = new String[][]{
				//h2ik, egknr, kv_nummer, pid
				new String[]{"1", "12", "12", "0000001234"},
				new String[]{"1", "12", "12", "0000001234"},
				new String[]{"1", "", "12", "0000001233"},
				new String[]{"1", "34", "", "0000001233"},
				new String[]{"1", "", "", "0000005678"},
				new String[]{"1", "22", "", "0000005677"},
			};
			
			private int i = 0;
			@Override
			public List<RowElement> getColumnNames() throws DataTableOperationException {
				List<RowElement> names = new ArrayList<>(4);
				names.add(new SimpleRowElement("h2ik", 0, TeradataColumnType.CHARACTER, "h2ik"));
				names.add(new SimpleRowElement("egk_nr", 1, TeradataColumnType.CHARACTER, "egk_nr"));
				names.add(new SimpleRowElement("kv_nummer", 2, TeradataColumnType.CHARACTER, "kv_nummer"));
				names.add(new SimpleRowElement("pid", 3, TeradataColumnType.CHARACTER, "pid"));

				return names;
			}

			@Override
			public List<RowElement> getNextRow(boolean ignoreMalformedRows) throws DataTableOperationException {
				String[] row = rows[i++];
				List<RowElement> converted = new ArrayList<>();
				converted.add(new SimpleRowElement("h2ik", 0, TeradataColumnType.CHARACTER, row[0]));
				converted.add(new SimpleRowElement("egknr", 1, TeradataColumnType.CHARACTER, row[1]));
				converted.add(new SimpleRowElement("kv_nummer", 2, TeradataColumnType.CHARACTER, row[2]));
				converted.add(new SimpleRowElement("pid", 3, TeradataColumnType.CHARACTER, row[3]));

				return converted;
			}

			@Override
			public boolean hasMoreRows() throws DataTableOperationException {
				return i < rows.length;
			}

			@Override
			public boolean open() throws DataTableOperationException {
				return hasMoreRows();
			}

			@Override
			public void close() throws DataTableOperationException {
			}

			@Override
			public String getName() {
				return "unfiltered pids";
			}
			
			@Override
			public String getDelimiter() {
				return "";
			}
		};
	}
	
	@Test
	public void removeRowsWithEntriesAlreadySeen() throws DataTableOperationException, JsonProcessingException, IOException {
		Configuration config = Configuration.loadFromJson("src/test/resources/configuration/decode/config.json"); 
		DataTable cleaned = new ProcessPidDecode(IOManager.of(config)).process(unfilteredPids, unwantedPids);
		assertTrue(cleaned.open());
		int rowIndex = 0;
		/*
		 * expected:
		 * 1;22;;5677
		 */
		while(cleaned.hasMoreRows()) {
			List<RowElement> row = cleaned.getNextRow(true);
			if(rowIndex == 0) {
				assertEquals("h2ik", row.get(0).getContent());
				assertEquals("egk_nr", row.get(1).getContent());
				assertEquals("kv_nummer", row.get(2).getContent());
				assertEquals("pid", row.get(3).getContent());
			} else {
				assertEquals("1", row.get(0).getContent());
				assertFalse(row.get(3).getContent().isEmpty());
				
				if(row.get(3).getContent().equalsIgnoreCase("0000005677")) {
					assertEquals("22", row.get(1).getContent());
					assertEquals("", row.get(2).getContent());
				}
			}
			rowIndex++;
		}
		
		assertEquals(3, rowIndex);
		cleaned.close();
	}

}
