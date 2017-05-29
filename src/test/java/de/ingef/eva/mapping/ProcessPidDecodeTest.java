package de.ingef.eva.mapping;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import de.ingef.eva.configuration.Configuration;
import de.ingef.eva.configuration.JsonConfigurationReader;
import de.ingef.eva.datasource.DataTable;
import de.ingef.eva.error.DataTableOperationException;

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
			public String[] getNextRow() throws DataTableOperationException {
				return new String[]{rows[i++]};
			}
			
			@Override
			public String getName() {
				return "unwantedPids";
			}
			
			@Override
			public List<String> getColumnTypes() throws DataTableOperationException {
				return null;
			}
			
			@Override
			public List<String> getColumnNames() throws DataTableOperationException {
				return null;
			}
			
			@Override
			public void close() throws DataTableOperationException {
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
			public List<String> getColumnNames() throws DataTableOperationException {
				List<String> names = new ArrayList<String>(4);
				names.add("h2ik");
				names.add("egk_nr");
				names.add("kv_nummer");
				names.add("pid");
				return names;
			}

			@Override
			public List<String> getColumnTypes() throws DataTableOperationException {
				return null;
			}

			@Override
			public String[] getNextRow() throws DataTableOperationException {
				return rows[i++];
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
		};
	}
	
	@Test
	public void testProcess() throws DataTableOperationException {
		Configuration config = new JsonConfigurationReader().ReadConfiguration("src/test/resources/configuration/decode/config.json");
		DataTable cleaned = new ProcessPidDecode(config).process(unfilteredPids, unwantedPids);
		assertTrue(cleaned.open());
//		assertTrue(cleaned.hasMoreRows());
		int rowIndex = 0;
		/*
		 * expected:
		 * 1;34;12;1233
		 * 1;22;;5677
		 */
		while(cleaned.hasMoreRows()) {
			String[] row = cleaned.getNextRow();
			if(rowIndex == 0) {
				assertEquals("h2ik", row[0]);
				assertEquals("egk_nr", row[1]);
				assertEquals("kv_nummer", row[2]);
				assertEquals("pid", row[3]);
			} else {
				assertEquals("1", row[0]);
				assertFalse(row[3].isEmpty());
				if(row[3].equalsIgnoreCase("0000001233")) {
					assertEquals("34", row[1]);
					assertEquals("12", row[2]);
				}
				if(row[3].equalsIgnoreCase("0000005677")) {
					assertEquals("22", row[1]);
					assertEquals("", row[2]);
				}
			}
			rowIndex++;
		}
		
		assertEquals(3, rowIndex);
	}

}
