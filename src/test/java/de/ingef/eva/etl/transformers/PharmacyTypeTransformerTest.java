package de.ingef.eva.etl.transformers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import de.ingef.eva.data.RowElement;
import de.ingef.eva.data.SimpleRowElement;
import de.ingef.eva.data.TeradataColumnType;
import de.ingef.eva.etl.PharmacyTypeTransformer;
import de.ingef.eva.etl.Row;
import de.ingef.eva.etl.Transformer;

public class PharmacyTypeTransformerTest {
	
	@Test
	public void appendEmptyStringIfPharmacyIdIsNull() {
		Transformer transformer = new PharmacyTypeTransformer("ADB", "AM_EVO");
		Row row = new Row(
				"ADB",
				"AM_EVO",
				Collections.singletonList(new SimpleRowElement(null, TeradataColumnType.CHARACTER)),
				Collections.singletonMap("apothekenik", 0)
		);
		Row transformedRow = transformer.transform(row);
		assertNotNull(transformedRow);
		assertEquals("ADB", transformedRow.getDb());
		assertEquals("AM_EVO", transformedRow.getTable());
		Map<String, Integer> indices = transformedRow.getColumnName2Index();
		assertTrue(indices.containsKey("apothekenik"));
		assertEquals(0, indices.get("apothekenik").intValue());
		assertTrue(indices.containsKey("apo_typ"));
		assertEquals(1, indices.get("apo_typ").intValue());
		List<RowElement> transformedColumns = transformedRow.getColumns(); 
		assertNull(transformedColumns.get(0).getContent());
		assertTrue(transformedColumns.get(1).getContent().isEmpty());
	}
	
	public void appendEmptyStringIfPharmacyIdIsEmpty() {
		Transformer transformer = new PharmacyTypeTransformer("ADB", "AM_EVO");
		Row row = new Row(
			"ADB",
			"AM_EVO",
			Collections.singletonList(new SimpleRowElement("", TeradataColumnType.CHARACTER)),
			Collections.singletonMap("apothekenik", 0)
		);
		Row transformedRow = transformer.transform(row);
		assertNotNull(transformedRow);
		assertEquals("ADB", transformedRow.getDb());
		assertEquals("AM_EVO", transformedRow.getTable());
		Map<String, Integer> indices = transformedRow.getColumnName2Index();
		assertTrue(indices.containsKey("apothekenik"));
		assertEquals(0, indices.get("apothekenik").intValue());
		assertTrue(indices.containsKey("apo_typ"));
		assertEquals(1, indices.get("apo_typ").intValue());
		List<RowElement> transformedColumns = transformedRow.getColumns(); 
		assertTrue(transformedColumns.get(0).getContent().isEmpty());
		assertTrue(transformedColumns.get(1).getContent().isEmpty());
	}
	
	@Test
	public void appendTypeHospital() {
		Transformer transformer = new PharmacyTypeTransformer("ADB", "AM_EVO");
		Row row = new Row(
			"ADB",
			"AM_EVO",
			Collections.singletonList(new SimpleRowElement("264637", TeradataColumnType.CHARACTER)),
			Collections.singletonMap("apothekenik", 0)
		);
		Row transformedRow = transformer.transform(row);
		assertNotNull(transformedRow);
		assertEquals("ADB", transformedRow.getDb());
		assertEquals("AM_EVO", transformedRow.getTable());
		Map<String, Integer> indices = transformedRow.getColumnName2Index();
		assertTrue(indices.containsKey("apothekenik"));
		assertEquals(0, indices.get("apothekenik").intValue());
		assertTrue(indices.containsKey("apo_typ"));
		assertEquals(1, indices.get("apo_typ").intValue());
		List<RowElement> transformedColumns = transformedRow.getColumns(); 
		assertEquals("264637", transformedColumns.get(0).getContent());
		assertEquals("26", transformedColumns.get(1).getContent());
	}
	
	@Test
	public void appendTypePharmacy() {
		Transformer transformer = new PharmacyTypeTransformer("ADB", "AM_EVO");
		Row row = new Row(
			"ADB",
			"AM_EVO",
			Collections.singletonList(new SimpleRowElement("304637", TeradataColumnType.CHARACTER)),
			Collections.singletonMap("apothekenik", 0)
		);
		Row transformedRow = transformer.transform(row);
		assertNotNull(transformedRow);
		assertEquals("ADB", transformedRow.getDb());
		assertEquals("AM_EVO", transformedRow.getTable());
		Map<String, Integer> indices = transformedRow.getColumnName2Index();
		assertTrue(indices.containsKey("apothekenik"));
		assertEquals(0, indices.get("apothekenik").intValue());
		assertTrue(indices.containsKey("apo_typ"));
		assertEquals(1, indices.get("apo_typ").intValue());
		List<RowElement> transformedColumns = transformedRow.getColumns(); 
		assertEquals("304637", transformedColumns.get(0).getContent());
		assertEquals("30", transformedColumns.get(1).getContent());
	}
	
	@Test
	public void appendTypeMiscellaneous() {
		Transformer transformer = new PharmacyTypeTransformer("ADB", "AM_EVO");
		Row row = new Row(
			"ADB",
			"AM_EVO",
			Collections.singletonList(new SimpleRowElement("114637", TeradataColumnType.CHARACTER)),
			Collections.singletonMap("apothekenik", 0)
		);
		Row transformedRow = transformer.transform(row);
		assertNotNull(transformedRow);
		assertEquals("ADB", transformedRow.getDb());
		assertEquals("AM_EVO", transformedRow.getTable());
		Map<String, Integer> indices = transformedRow.getColumnName2Index();
		assertTrue(indices.containsKey("apothekenik"));
		assertEquals(0, indices.get("apothekenik").intValue());
		assertTrue(indices.containsKey("apo_typ"));
		assertEquals(1, indices.get("apo_typ").intValue());
		List<RowElement> transformedColumns = transformedRow.getColumns(); 
		assertEquals("114637", transformedColumns.get(0).getContent());
		assertEquals("00", transformedColumns.get(1).getContent());
	}
	
	@Test
	public void skipRowWhenDBDoesNotMatch() {
		Transformer transformer = new PharmacyTypeTransformer("ADB", "AM_EVO");
		Row row = new Row(
			"FDB",
			"AM_EVO",
			Collections.singletonList(new SimpleRowElement("264637", TeradataColumnType.CHARACTER)),
			Collections.singletonMap("apothekenik", 0)
		);
		Row transformedRow = transformer.transform(row);
		assertNotNull(transformedRow);
		assertEquals("FDB", transformedRow.getDb());
		assertEquals("AM_EVO", transformedRow.getTable());
		Map<String, Integer> indices = transformedRow.getColumnName2Index();
		assertEquals(1, indices.size());
		assertTrue(indices.containsKey("apothekenik"));
		assertEquals(0, indices.get("apothekenik").intValue());
		List<RowElement> transformedColumns = transformedRow.getColumns();
		assertEquals(1, transformedColumns.size());
		assertEquals("264637", transformedColumns.get(0).getContent());
	}
	
	@Test
	public void skipRowWhenTableDoesNotMatch() {
		Transformer transformer = new PharmacyTypeTransformer("FDB", "HiMi");
		Row row = new Row(
			"FDB",
			"HeMi",
			Collections.singletonList(new SimpleRowElement("264637", TeradataColumnType.CHARACTER)),
			Collections.singletonMap("apothekenik", 0)
		);
		Row transformedRow = transformer.transform(row);
		assertNotNull(transformedRow);
		assertEquals("FDB", transformedRow.getDb());
		assertEquals("HeMi", transformedRow.getTable());
		Map<String, Integer> indices = transformedRow.getColumnName2Index();
		assertEquals(1, indices.size());
		assertTrue(indices.containsKey("apothekenik"));
		assertEquals(0, indices.get("apothekenik").intValue());
		List<RowElement> transformedColumns = transformedRow.getColumns();
		assertEquals(1, transformedColumns.size());
		assertEquals("264637", transformedColumns.get(0).getContent());
	}
}
