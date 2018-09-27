package de.ingef.eva.etl.transformers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;

import de.ingef.eva.configuration.append.AppendOrder;
import de.ingef.eva.data.RowElement;
import de.ingef.eva.data.SimpleRowElement;
import de.ingef.eva.data.TeradataColumnType;
import de.ingef.eva.etl.Row;

public class StaticColumnAppenderTransformerTest {

	private static Row row;
	
	@BeforeClass
	public static void createRow() {
		List<RowElement> columns = new ArrayList<>(1);
		columns.add(new SimpleRowElement("1337", TeradataColumnType.INTEGER));
		Map<String, Integer> column2Index = new HashMap<>(1);
		column2Index.put("id", 0);
		row = new Row("ADB", "AVK_ADB_T_AM_EVO", columns, column2Index);
	}
	
	@Test
	public void addConstantIfDBMatches() {
		StaticColumnAppenderTransformer transformer = new StaticColumnAppenderTransformer("adb", null, "h2ik", "999999999", AppendOrder.FIRST, null);
		Row transformedRow = transformer.transform(row);
		assertNotNull(transformedRow);
		assertEquals("ADB", transformedRow.getDb());
		assertEquals("AVK_ADB_T_AM_EVO", transformedRow.getTable());
		
		List<RowElement> columns = transformedRow.getColumns();
		assertNotNull(columns);
		assertEquals(2, columns.size());
		
		RowElement ik = columns.get(0);
		assertNotNull(ik);
		assertEquals("999999999", ik.getContent());
		assertEquals(TeradataColumnType.CHARACTER, ik.getType());
		
		RowElement id = columns.get(1);
		assertNotNull(id);
		assertEquals("1337", id.getContent());
		assertEquals(TeradataColumnType.INTEGER, id.getType());
		
		Map<String, Integer> column2Index = transformedRow.getColumnName2Index();
		assertNotNull(column2Index);
		assertEquals(2, column2Index.size());
		assertTrue(column2Index.containsKey("h2ik"));
		assertEquals(0, (int)column2Index.get("h2ik"));
		assertTrue(column2Index.containsKey("id"));
		assertEquals(1, (int)column2Index.get("id"));
	}
	
	@Test
	public void addConstantIfTableMatches() {
		StaticColumnAppenderTransformer transformer = new StaticColumnAppenderTransformer(null, "am_evo", "h2ik", "999999999", AppendOrder.FIRST, null);
		Row transformedRow = transformer.transform(row);
		assertNotNull(transformedRow);
		assertEquals("ADB", transformedRow.getDb());
		assertEquals("AVK_ADB_T_AM_EVO", transformedRow.getTable());
		
		List<RowElement> columns = transformedRow.getColumns();
		assertNotNull(columns);
		assertEquals(2, columns.size());
		
		RowElement ik = columns.get(0);
		assertNotNull(ik);
		assertEquals("999999999", ik.getContent());
		assertEquals(TeradataColumnType.CHARACTER, ik.getType());
		
		RowElement id = columns.get(1);
		assertNotNull(id);
		assertEquals("1337", id.getContent());
		assertEquals(TeradataColumnType.INTEGER, id.getType());
		
		Map<String, Integer> column2Index = transformedRow.getColumnName2Index();
		assertNotNull(column2Index);
		assertEquals(2, column2Index.size());
		assertTrue(column2Index.containsKey("h2ik"));
		assertEquals(0, (int)column2Index.get("h2ik"));
		assertTrue(column2Index.containsKey("id"));
		assertEquals(1, (int)column2Index.get("id"));
	}
	
	@Test
	public void addConstantIfDBAndTableMatch() {
		StaticColumnAppenderTransformer transformer = new StaticColumnAppenderTransformer("adb", "am_evo", "h2ik", "999999999", AppendOrder.FIRST, null);
		Row transformedRow = transformer.transform(row);
		assertNotNull(transformedRow);
		assertEquals("ADB", transformedRow.getDb());
		assertEquals("AVK_ADB_T_AM_EVO", transformedRow.getTable());
		
		List<RowElement> columns = transformedRow.getColumns();
		assertNotNull(columns);
		assertEquals(2, columns.size());
		
		RowElement ik = columns.get(0);
		assertNotNull(ik);
		assertEquals("999999999", ik.getContent());
		assertEquals(TeradataColumnType.CHARACTER, ik.getType());
		
		RowElement id = columns.get(1);
		assertNotNull(id);
		assertEquals("1337", id.getContent());
		assertEquals(TeradataColumnType.INTEGER, id.getType());
		
		Map<String, Integer> column2Index = transformedRow.getColumnName2Index();
		assertNotNull(column2Index);
		assertEquals(2, column2Index.size());
		assertTrue(column2Index.containsKey("h2ik"));
		assertEquals(0, (int)column2Index.get("h2ik"));
		assertTrue(column2Index.containsKey("id"));
		assertEquals(1, (int)column2Index.get("id"));
	}
	
	@Test
	public void skipRowIfTableIsExcluded() {
		List<String> excludeTables = new ArrayList<>(1);
		excludeTables.contains("am_evo");
		
		StaticColumnAppenderTransformer transformer = new StaticColumnAppenderTransformer("adb", "am_evo", "h2ik", "999999999", AppendOrder.FIRST, excludeTables);
		Row transformedRow = transformer.transform(row);
		assertNotNull(transformedRow);
		assertEquals("ADB", transformedRow.getDb());
		assertEquals("AVK_ADB_T_AM_EVO", transformedRow.getTable());
		
		List<RowElement> columns = transformedRow.getColumns();
		assertNotNull(columns);
		assertEquals(2, columns.size());
		
		RowElement ik = columns.get(0);
		assertNotNull(ik);
		assertEquals("999999999", ik.getContent());
		assertEquals(TeradataColumnType.CHARACTER, ik.getType());
		
		RowElement id = columns.get(1);
		assertNotNull(id);
		assertEquals("1337", id.getContent());
		assertEquals(TeradataColumnType.INTEGER, id.getType());
		
		Map<String, Integer> column2Index = transformedRow.getColumnName2Index();
		assertNotNull(column2Index);
		assertEquals(2, column2Index.size());
		assertTrue(column2Index.containsKey("h2ik"));
		assertEquals(0, (int)column2Index.get("h2ik"));
		assertTrue(column2Index.containsKey("id"));
		assertEquals(1, (int)column2Index.get("id"));
	}
}
