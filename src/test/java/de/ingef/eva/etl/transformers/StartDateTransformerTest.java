package de.ingef.eva.etl.transformers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.time.LocalDate;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import de.ingef.eva.data.RowElement;
import de.ingef.eva.data.SimpleRowElement;
import de.ingef.eva.data.TeradataColumnType;
import de.ingef.eva.etl.Row;

public class StartDateTransformerTest {

	@Test
	public void useEndDateAsStartDateIfInvalidDayCountValues() {
		List<RowElement> columns = Arrays.asList(
				new SimpleRowElement("2018-08-02", TeradataColumnType.DATE),
				new SimpleRowElement("(()3", TeradataColumnType.CHARACTER)
			);
		Map<String, Integer> indices = new HashMap<>(2);
		indices.put("leistungs_ende", 0);
		indices.put("kg_tage", 1);
		Row row = new Row("db", "table", columns, indices);
		Row transformedRow = new StartDateTransformer("db", "table", "leistungs_ende", "kg_tage", "kg_beginn").transform(row);
		assertNotNull(transformedRow);
		assertEquals(row.getDb(), transformedRow.getDb());
		assertEquals(row.getTable(), transformedRow.getTable());
		Map<String, Integer> transformedIndices = transformedRow.getColumnName2Index();
		assertNotNull(transformedIndices);
		assertEquals(row.getColumnName2Index().size() + 1, transformedIndices.size());
		assertEquals(0, transformedIndices.get("leistungs_ende").intValue());
		assertEquals(1, transformedIndices.get("kg_tage").intValue());
		assertEquals(2, transformedIndices.get("kg_beginn").intValue());
		List<RowElement> transformedColumns = transformedRow.getColumns();
		assertEquals(3, transformedColumns.size());
		assertEquals(columns.get(0).getContent(), transformedColumns.get(0).getContent());
		assertEquals(columns.get(1).getContent(), transformedColumns.get(1).getContent());
		assertEquals(columns.get(0).getContent(), transformedColumns.get(2).getContent());
	}

	@Test
	public void useEndDateAsStartDateIfDayCountIsZero() {
		List<RowElement> columns = Arrays.asList(
				new SimpleRowElement("2018-08-02", TeradataColumnType.DATE),
				new SimpleRowElement("0", TeradataColumnType.CHARACTER)
			);
		Map<String, Integer> indices = new HashMap<>(2);
		indices.put("leistungs_ende", 0);
		indices.put("kg_tage", 1);
		Row row = new Row("db", "table", columns, indices);
		Row transformedRow = new StartDateTransformer("db", "table", "leistungs_ende", "kg_tage", "kg_beginn").transform(row);
		assertNotNull(transformedRow);
		assertEquals(row.getDb(), transformedRow.getDb());
		assertEquals(row.getTable(), transformedRow.getTable());
		Map<String, Integer> transformedIndices = transformedRow.getColumnName2Index();
		assertNotNull(transformedIndices);
		assertEquals(row.getColumnName2Index().size() + 1, transformedIndices.size());
		assertEquals(0, transformedIndices.get("leistungs_ende").intValue());
		assertEquals(1, transformedIndices.get("kg_tage").intValue());
		assertEquals(2, transformedIndices.get("kg_beginn").intValue());
		List<RowElement> transformedColumns = transformedRow.getColumns();
		assertEquals(3, transformedColumns.size());
		assertEquals(columns.get(0).getContent(), transformedColumns.get(0).getContent());
		assertEquals(columns.get(1).getContent(), transformedColumns.get(1).getContent());
		assertEquals("2018-08-02", transformedColumns.get(2).getContent());
	}
	
	@Test
	public void includeEndDateInTimeSpan() {
		List<RowElement> columns = Arrays.asList(
				new SimpleRowElement("2018-08-02", TeradataColumnType.DATE),
				new SimpleRowElement("2", TeradataColumnType.CHARACTER)
			);
		Map<String, Integer> indices = new HashMap<>(2);
		indices.put("leistungs_ende", 0);
		indices.put("kg_tage", 1);
		Row row = new Row("db", "table", columns, indices);
		Row transformedRow = new StartDateTransformer("db", "table", "leistungs_ende", "kg_tage", "kg_beginn").transform(row);
		assertNotNull(transformedRow);
		assertEquals(row.getDb(), transformedRow.getDb());
		assertEquals(row.getTable(), transformedRow.getTable());
		Map<String, Integer> transformedIndices = transformedRow.getColumnName2Index();
		assertNotNull(transformedIndices);
		assertEquals(row.getColumnName2Index().size() + 1, transformedIndices.size());
		assertEquals(0, transformedIndices.get("leistungs_ende").intValue());
		assertEquals(1, transformedIndices.get("kg_tage").intValue());
		assertEquals(2, transformedIndices.get("kg_beginn").intValue());
		List<RowElement> transformedColumns = transformedRow.getColumns();
		assertEquals(3, transformedColumns.size());
		assertEquals(columns.get(0).getContent(), transformedColumns.get(0).getContent());
		assertEquals(columns.get(1).getContent(), transformedColumns.get(1).getContent());
		assertEquals(LocalDate.parse("2018-08-01"), LocalDate.parse(transformedColumns.get(2).getContent()));
	}
	
	@Test
	public void emptyStartDateIfEndDateIsMissing() {
		List<RowElement> columns = Arrays.asList(
				new SimpleRowElement("2", TeradataColumnType.CHARACTER)
			);
		Map<String, Integer> indices = new HashMap<>(1);
		indices.put("kg_tage", 0);
		Row row = new Row("db", "table", columns, indices);
		Row transformedRow = new StartDateTransformer("db", "table", "leistungs_ende", "kg_tage", "kg_beginn").transform(row);
		assertNotNull(transformedRow);
		assertEquals(row.getDb(), transformedRow.getDb());
		assertEquals(row.getTable(), transformedRow.getTable());
		Map<String, Integer> transformedIndices = transformedRow.getColumnName2Index();
		assertNotNull(transformedIndices);
		assertEquals(row.getColumnName2Index().size() + 1, transformedIndices.size());
		assertEquals(0, transformedIndices.get("kg_tage").intValue());
		assertEquals(1, transformedIndices.get("kg_beginn").intValue());
		List<RowElement> transformedColumns = transformedRow.getColumns();
		assertEquals(2, transformedColumns.size());
		assertEquals(columns.get(0).getContent(), transformedColumns.get(0).getContent());
		assertEquals("", transformedColumns.get(1).getContent());
	}
	
	@Test
	public void emptyStartDateIfKgDaysAreMissing() {
		List<RowElement> columns = Arrays.asList(
				new SimpleRowElement("2018-08-02", TeradataColumnType.DATE)
			);
		Map<String, Integer> indices = new HashMap<>(1);
		indices.put("leistungs_ende", 0);
		Row row = new Row("db", "table", columns, indices);
		Row transformedRow = new StartDateTransformer("db", "table", "leistungs_ende", "kg_tage", "kg_beginn").transform(row);
		assertNotNull(transformedRow);
		assertEquals(row.getDb(), transformedRow.getDb());
		assertEquals(row.getTable(), transformedRow.getTable());
		Map<String, Integer> transformedIndices = transformedRow.getColumnName2Index();
		assertNotNull(transformedIndices);
		assertEquals(row.getColumnName2Index().size() + 1, transformedIndices.size());
		assertEquals(0, transformedIndices.get("leistungs_ende").intValue());
		assertEquals(1, transformedIndices.get("kg_beginn").intValue());
		List<RowElement> transformedColumns = transformedRow.getColumns();
		assertEquals(2, transformedColumns.size());
		assertEquals(columns.get(0).getContent(), transformedColumns.get(0).getContent());
		assertEquals("", transformedColumns.get(1).getContent());
	}
}
