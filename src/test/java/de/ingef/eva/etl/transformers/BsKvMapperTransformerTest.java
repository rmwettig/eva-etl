package de.ingef.eva.etl.transformers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import de.ingef.eva.data.RowElement;
import de.ingef.eva.data.SimpleRowElement;
import de.ingef.eva.data.TeradataColumnType;
import de.ingef.eva.etl.BsKvMapperTransformer;
import de.ingef.eva.etl.Row;

public class BsKvMapperTransformerTest {

	@Test
	public void ignoreRowNotFromEvoTable() {
		List<RowElement> columns = new ArrayList<>(1);
		columns.add(new SimpleRowElement("123456789", TeradataColumnType.INTEGER));
		Map<String, Integer> columnName2Index = new HashMap<>(1);
		columnName2Index.put("id", 0);
		Row row = new Row("db", "kh_diagnose", columns, columnName2Index);
		Row transformedRow = new BsKvMapperTransformer().transform(row);
		
		assertNotNull(transformedRow);
		assertEquals(row.getDb(), transformedRow.getDb());
		assertEquals(row.getTable(), transformedRow.getTable());
		assertEquals(row.getColumns().size(), transformedRow.getColumns().size());
		assertEquals(row.getColumns().get(0).getContent(), transformedRow.getColumns().get(0).getContent());
		assertEquals(row.getColumnName2Index().size(), transformedRow.getColumnName2Index().size());
		assertTrue(transformedRow.getColumnName2Index().containsKey("id"));
		assertEquals(row.getColumnName2Index().get("id"), transformedRow.getColumnName2Index().get("id"));
	}
	
	@Test
	public void appendEmptyStringIfBsColumnIsMissing() {
		List<RowElement> columns = new ArrayList<>(1);
		columns.add(new SimpleRowElement("123456789", TeradataColumnType.INTEGER));
		Map<String, Integer> columnName2Index = new HashMap<>(1);
		columnName2Index.put("id", 0);
		Row row = new Row("db", "am_evo", columns, columnName2Index);
		Row transformedRow = new BsKvMapperTransformer().transform(row);
		
		assertNotNull(transformedRow);
		assertEquals(row.getDb(), transformedRow.getDb());
		assertEquals(row.getTable(), transformedRow.getTable());
		assertEquals(2, transformedRow.getColumns().size());
		assertEquals(row.getColumns().get(0).getContent(), transformedRow.getColumns().get(0).getContent());
		assertEquals("", transformedRow.getColumns().get(1).getContent());
		assertEquals(2, transformedRow.getColumnName2Index().size());
		
		assertTrue(transformedRow.getColumnName2Index().containsKey("id"));
		assertEquals(row.getColumnName2Index().get("id"), transformedRow.getColumnName2Index().get("id"));
		
		assertFalse(row.getColumnName2Index().containsKey("kv"));
		assertTrue(transformedRow.getColumnName2Index().containsKey("kv"));
		assertEquals(1, transformedRow.getColumnName2Index().get("kv").intValue());
	}
	
	@Test
	public void appendEmptyStringIfBsIsNull() {
		List<RowElement> columns = new ArrayList<>(1);
		columns.add(new SimpleRowElement("123456789", TeradataColumnType.INTEGER));
		columns.add(new SimpleRowElement(null, TeradataColumnType.CHARACTER));
		Map<String, Integer> columnName2Index = new HashMap<>(1);
		columnName2Index.put("id", 0);
		columnName2Index.put("bs_nr", 1);
		Row row = new Row("db", "am_evo", columns, columnName2Index);
		Row transformedRow = new BsKvMapperTransformer().transform(row);
		
		assertNotNull(transformedRow);
		assertEquals(row.getDb(), transformedRow.getDb());
		assertEquals(row.getTable(), transformedRow.getTable());
		assertEquals(3, transformedRow.getColumns().size());
		assertEquals(row.getColumns().get(0).getContent(), transformedRow.getColumns().get(0).getContent());
		assertEquals(row.getColumns().get(1).getContent(), transformedRow.getColumns().get(1).getContent());
		assertEquals(3, transformedRow.getColumnName2Index().size());
		
		assertTrue(transformedRow.getColumnName2Index().containsKey("id"));
		assertEquals(row.getColumnName2Index().get("id"), transformedRow.getColumnName2Index().get("id"));
		
		assertTrue(transformedRow.getColumnName2Index().containsKey("bs_nr"));
		assertEquals(1, transformedRow.getColumnName2Index().get("bs_nr").intValue());
		
		assertFalse(row.getColumnName2Index().containsKey("kv"));
		assertTrue(transformedRow.getColumnName2Index().containsKey("kv"));
		assertEquals(2, transformedRow.getColumnName2Index().get("kv").intValue());
		
		assertEquals(row.getColumns().get(1), transformedRow.getColumns().get(1));
		assertEquals("", transformedRow.getColumns().get(2).getContent());
	}
	
	@Test
	public void appendEmptyStringIfBsIsEmptyString() {
		List<RowElement> columns = new ArrayList<>(1);
		columns.add(new SimpleRowElement("123456789", TeradataColumnType.INTEGER));
		columns.add(new SimpleRowElement("", TeradataColumnType.CHARACTER));
		Map<String, Integer> columnName2Index = new HashMap<>(1);
		columnName2Index.put("id", 0);
		columnName2Index.put("bs_nr", 1);
		Row row = new Row("db", "am_evo", columns, columnName2Index);
		Row transformedRow = new BsKvMapperTransformer().transform(row);
		
		assertNotNull(transformedRow);
		assertEquals(row.getDb(), transformedRow.getDb());
		assertEquals(row.getTable(), transformedRow.getTable());
		assertEquals(3, transformedRow.getColumns().size());
		assertEquals(row.getColumns().get(0).getContent(), transformedRow.getColumns().get(0).getContent());
		assertEquals("", transformedRow.getColumns().get(1).getContent());
		assertEquals(3, transformedRow.getColumnName2Index().size());
		
		assertTrue(transformedRow.getColumnName2Index().containsKey("id"));
		assertEquals(row.getColumnName2Index().get("id"), transformedRow.getColumnName2Index().get("id"));
		
		assertTrue(transformedRow.getColumnName2Index().containsKey("bs_nr"));
		assertEquals(1, transformedRow.getColumnName2Index().get("bs_nr").intValue());
		
		assertFalse(row.getColumnName2Index().containsKey("kv"));
		assertTrue(transformedRow.getColumnName2Index().containsKey("kv"));
		assertEquals(2, transformedRow.getColumnName2Index().get("kv").intValue());
		
		assertEquals(row.getColumns().get(1), transformedRow.getColumns().get(1));
		assertEquals("", transformedRow.getColumns().get(2).getContent());
	}
	
	@Test
	public void appendEmptyStringIfBsHasLengthOne() {
		List<RowElement> columns = new ArrayList<>(1);
		columns.add(new SimpleRowElement("123456789", TeradataColumnType.INTEGER));
		columns.add(new SimpleRowElement("1", TeradataColumnType.CHARACTER));
		Map<String, Integer> columnName2Index = new HashMap<>(1);
		columnName2Index.put("id", 0);
		columnName2Index.put("bs_nr", 1);
		Row row = new Row("db", "am_evo", columns, columnName2Index);
		Row transformedRow = new BsKvMapperTransformer().transform(row);
		
		assertNotNull(transformedRow);
		assertEquals(row.getDb(), transformedRow.getDb());
		assertEquals(row.getTable(), transformedRow.getTable());
		assertEquals(3, transformedRow.getColumns().size());
		assertEquals(row.getColumns().get(0).getContent(), transformedRow.getColumns().get(0).getContent());
		assertEquals(row.getColumns().get(1).getContent(), transformedRow.getColumns().get(1).getContent());
		assertEquals(3, transformedRow.getColumnName2Index().size());
		
		assertTrue(transformedRow.getColumnName2Index().containsKey("id"));
		assertEquals(row.getColumnName2Index().get("id"), transformedRow.getColumnName2Index().get("id"));
		
		assertTrue(transformedRow.getColumnName2Index().containsKey("bs_nr"));
		assertEquals(1, transformedRow.getColumnName2Index().get("bs_nr").intValue());
		
		assertFalse(row.getColumnName2Index().containsKey("kv"));
		assertTrue(transformedRow.getColumnName2Index().containsKey("kv"));
		assertEquals(2, transformedRow.getColumnName2Index().get("kv").intValue());
		
		assertEquals(row.getColumns().get(1), transformedRow.getColumns().get(1));
		assertEquals("", transformedRow.getColumns().get(2).getContent());
	}
	
	@Test
	public void appendMajorKvIfBsIsValid() {
		List<RowElement> columns = new ArrayList<>(1);
		columns.add(new SimpleRowElement("123456789", TeradataColumnType.INTEGER));
		columns.add(new SimpleRowElement("15938", TeradataColumnType.CHARACTER));
		Map<String, Integer> columnName2Index = new HashMap<>(1);
		columnName2Index.put("id", 0);
		columnName2Index.put("bs_nr", 1);
		Row row = new Row("db", "am_evo", columns, columnName2Index);
		Row transformedRow = new BsKvMapperTransformer().transform(row);
		
		assertNotNull(transformedRow);
		assertEquals(row.getDb(), transformedRow.getDb());
		assertEquals(row.getTable(), transformedRow.getTable());
		assertEquals(3, transformedRow.getColumns().size());
		assertEquals(row.getColumns().get(0).getContent(), transformedRow.getColumns().get(0).getContent());
		assertEquals("15938", transformedRow.getColumns().get(1).getContent());
		assertEquals(3, transformedRow.getColumnName2Index().size());
		
		assertTrue(transformedRow.getColumnName2Index().containsKey("id"));
		assertEquals(row.getColumnName2Index().get("id"), transformedRow.getColumnName2Index().get("id"));
		
		assertTrue(transformedRow.getColumnName2Index().containsKey("bs_nr"));
		assertEquals(1, transformedRow.getColumnName2Index().get("bs_nr").intValue());
		
		assertFalse(row.getColumnName2Index().containsKey("kv"));
		assertTrue(transformedRow.getColumnName2Index().containsKey("kv"));
		assertEquals(2, transformedRow.getColumnName2Index().get("kv").intValue());
		
		assertEquals(row.getColumns().get(1), transformedRow.getColumns().get(1));
		assertEquals("17", transformedRow.getColumns().get(2).getContent());
	}
}
