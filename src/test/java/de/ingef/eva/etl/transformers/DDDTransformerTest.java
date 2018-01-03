package de.ingef.eva.etl.transformers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import de.ingef.eva.configuration.append.AppendConfiguration;
import de.ingef.eva.data.RowElement;
import de.ingef.eva.data.SimpleRowElement;
import de.ingef.eva.data.TeradataColumnType;
import de.ingef.eva.etl.DDDTransformer;
import de.ingef.eva.etl.Row;
import de.ingef.eva.etl.Transformer;
import de.ingef.eva.etl.WidoColumn;

public class DDDTransformerTest {

	@Test
	public void useDataOfLastDefinedSource() throws JsonParseException, JsonMappingException, IOException {
		AppendConfiguration config = new ObjectMapper().readValue(new File("src/test/resources/configuration/etl/dynamicAppending/DDD_without_name.json"), AppendConfiguration.class);
		Transformer transformer = DDDTransformer.of(config.getTargetDb(), config.getTargetTable(), config.getKeyColumn(), config.getSources());
		List<RowElement> columns = Arrays.asList(
				new SimpleRowElement("00000001", TeradataColumnType.CHARACTER),
				new SimpleRowElement("abcde", TeradataColumnType.CHARACTER),
				new SimpleRowElement("10", TeradataColumnType.CHARACTER),
				new SimpleRowElement("2010-04-01", TeradataColumnType.CHARACTER)
		);
		Map<String,Integer> columnNames2Index = new HashMap<>(3);
		columnNames2Index.put("pzn", 0);
		columnNames2Index.put("atc", 1);
		columnNames2Index.put("Anzahl_Packungen".toLowerCase(), 2);
		columnNames2Index.put("Verordnungsdatum".toLowerCase(), 3);
		Row row = new Row(config.getTargetDb(), config.getTargetTable(), columns, columnNames2Index);
		
		Row transformedRow = transformer.transform(row);
		assertNotNull(transformedRow);
		assertEquals(row.getDb(), transformedRow.getDb());
		assertEquals(row.getTable(), transformedRow.getTable());
		
		Map<String,Integer> transformedIndices = transformedRow.getColumnName2Index();
		assertNotNull(transformedIndices);
		assertFalse(transformedIndices.isEmpty());
		
		assertTrue(transformedIndices.containsKey("pzn"));
		assertEquals(0, (int)transformedIndices.get("pzn"));
		
		assertTrue(transformedIndices.containsKey("atc"));
		assertEquals(1, (int)transformedIndices.get("atc"));
		
		assertTrue(transformedIndices.containsKey("Anzahl_Packungen".toLowerCase()));
		assertEquals(2, (int)transformedIndices.get("Anzahl_Packungen".toLowerCase()));
		
		assertTrue(transformedIndices.containsKey("Verordnungsdatum".toLowerCase()));
		assertEquals(3, (int)transformedIndices.get("Verordnungsdatum".toLowerCase()));
		
		assertTrue(transformedIndices.containsKey(WidoColumn.DDDPK.getLabel()));
		assertEquals(4, (int)transformedIndices.get(WidoColumn.DDDPK.getLabel()));

		assertTrue(transformedIndices.containsKey(WidoColumn.APPFORM.getLabel()));
		assertEquals(5, (int)transformedIndices.get(WidoColumn.APPFORM.getLabel()));
		
		assertTrue(transformedIndices.containsKey(WidoColumn.GENERIC.getLabel()));
		assertEquals(6, (int)transformedIndices.get(WidoColumn.GENERIC.getLabel()));
		
		List<RowElement> transformedColumns = transformedRow.getColumns();
		assertNotNull(transformedColumns);
		assertFalse(transformedColumns.isEmpty());
		assertEquals(transformedIndices.size(), transformedColumns.size());
		
		RowElement pzn = transformedColumns.get(0);
		assertNotNull(pzn);
		assertEquals("00000001", pzn.getContent());
		
		RowElement atc = transformedColumns.get(1);
		assertNotNull(atc);
		assertEquals("abcde", atc.getContent());
		
		RowElement packages = transformedColumns.get(2);
		assertNotNull(packages);
		assertEquals("10", packages.getContent());
		
		RowElement prescriptionDate = transformedColumns.get(3);
		assertNotNull(prescriptionDate);
		assertEquals("2010-04-01", prescriptionDate.getContent());
		
		RowElement ddd = transformedColumns.get(4);
		assertNotNull(ddd);
		assertEquals("10.0", ddd.getContent());
		
		RowElement appForm = transformedColumns.get(5);
		assertNotNull(appForm);
		assertEquals("0", appForm.getContent());
		
		RowElement generic = transformedColumns.get(6);
		assertNotNull(generic);
		assertEquals("1", generic.getContent());
	}
	
	@Test
	public void useDataFromSecondaryFiles() throws JsonParseException, JsonMappingException, IOException {
		AppendConfiguration config = new ObjectMapper().readValue(new File("src/test/resources/configuration/etl/dynamicAppending/DDD_without_name.json"), AppendConfiguration.class);
		Transformer transformer = DDDTransformer.of(config.getTargetDb(), config.getTargetTable(), config.getKeyColumn(), config.getSources());
		List<RowElement> columns = Arrays.asList(
				new SimpleRowElement("00000004", TeradataColumnType.CHARACTER),
				new SimpleRowElement("abcde", TeradataColumnType.CHARACTER),
				new SimpleRowElement("10", TeradataColumnType.CHARACTER),
				new SimpleRowElement("2010-04-01", TeradataColumnType.CHARACTER)
		);
		Map<String,Integer> columnNames2Index = new HashMap<>(4);
		columnNames2Index.put("pzn", 0);
		columnNames2Index.put("atc", 1);
		columnNames2Index.put("Anzahl_Packungen".toLowerCase(), 2);
		columnNames2Index.put("Verordnungsdatum".toLowerCase(), 3);
		Row row = new Row(config.getTargetDb(), config.getTargetTable(), columns, columnNames2Index);
		
		Row transformedRow = transformer.transform(row);
		assertNotNull(transformedRow);
		assertEquals(row.getDb(), transformedRow.getDb());
		assertEquals(row.getTable(), transformedRow.getTable());
		
		Map<String,Integer> transformedIndices = transformedRow.getColumnName2Index();
		assertNotNull(transformedIndices);
		assertFalse(transformedIndices.isEmpty());
		
		assertTrue(transformedIndices.containsKey("pzn"));
		assertEquals(0, (int)transformedIndices.get("pzn"));
		
		assertTrue(transformedIndices.containsKey("atc"));
		assertEquals(1, (int)transformedIndices.get("atc"));
		
		assertTrue(transformedIndices.containsKey("Anzahl_Packungen".toLowerCase()));
		assertEquals(2, (int)transformedIndices.get("Anzahl_Packungen".toLowerCase()));
		
		assertTrue(transformedIndices.containsKey("Verordnungsdatum".toLowerCase()));
		assertEquals(3, (int)transformedIndices.get("Verordnungsdatum".toLowerCase()));
		
		assertTrue(transformedIndices.containsKey(WidoColumn.DDDPK.getLabel()));
		assertEquals(4, (int)transformedIndices.get(WidoColumn.DDDPK.getLabel()));

		assertTrue(transformedIndices.containsKey(WidoColumn.APPFORM.getLabel()));
		assertEquals(5, (int)transformedIndices.get(WidoColumn.APPFORM.getLabel()));
		
		assertTrue(transformedIndices.containsKey(WidoColumn.GENERIC.getLabel()));
		assertEquals(6, (int)transformedIndices.get(WidoColumn.GENERIC.getLabel()));
		
		List<RowElement> transformedColumns = transformedRow.getColumns();
		assertNotNull(transformedColumns);
		assertFalse(transformedColumns.isEmpty());
		assertEquals(transformedIndices.size(), transformedColumns.size());
		
		RowElement pzn = transformedColumns.get(0);
		assertNotNull(pzn);
		assertEquals("00000004", pzn.getContent());
		
		RowElement atc = transformedColumns.get(1);
		assertNotNull(atc);
		assertEquals("abcde", atc.getContent());
		
		RowElement packages = transformedColumns.get(2);
		assertNotNull(packages);
		assertEquals("10", packages.getContent());
		
		RowElement prescriptionDate = transformedColumns.get(3);
		assertNotNull(prescriptionDate);
		assertEquals("2010-04-01", prescriptionDate.getContent());
		
		RowElement ddd = transformedColumns.get(4);
		assertNotNull(ddd);
		assertEquals("4.0", ddd.getContent());
		
		RowElement appForm = transformedColumns.get(5);
		assertNotNull(appForm);
		assertEquals("", appForm.getContent());
		
		RowElement generic = transformedColumns.get(6);
		assertNotNull(generic);
		assertEquals("0", generic.getContent());
	}
	
	@Test
	public void useDataFromMainFile() throws JsonParseException, JsonMappingException, IOException {
		AppendConfiguration config = new ObjectMapper().readValue(new File("src/test/resources/configuration/etl/dynamicAppending/DDD_without_name.json"), AppendConfiguration.class);
		Transformer transformer = DDDTransformer.of(config.getTargetDb(), config.getTargetTable(), config.getKeyColumn(), config.getSources());
		List<RowElement> columns = Arrays.asList(
				new SimpleRowElement("00000002", TeradataColumnType.CHARACTER),
				new SimpleRowElement("abcde", TeradataColumnType.CHARACTER),
				new SimpleRowElement("10", TeradataColumnType.CHARACTER),
				new SimpleRowElement("2010-04-01", TeradataColumnType.CHARACTER)
		);
		Map<String,Integer> columnNames2Index = new HashMap<>(3);
		columnNames2Index.put("pzn", 0);
		columnNames2Index.put("atc", 1);
		columnNames2Index.put("Anzahl_Packungen".toLowerCase(), 2);
		columnNames2Index.put("Verordnungsdatum".toLowerCase(), 3);
		Row row = new Row(config.getTargetDb(), config.getTargetTable(), columns, columnNames2Index);
		
		Row transformedRow = transformer.transform(row);
		assertNotNull(transformedRow);
		assertEquals(row.getDb(), transformedRow.getDb());
		assertEquals(row.getTable(), transformedRow.getTable());
		
		Map<String,Integer> transformedIndices = transformedRow.getColumnName2Index();
		assertNotNull(transformedIndices);
		assertFalse(transformedIndices.isEmpty());
		
		assertTrue(transformedIndices.containsKey("pzn"));
		assertEquals(0, (int)transformedIndices.get("pzn"));
		
		assertTrue(transformedIndices.containsKey("atc"));
		assertEquals(1, (int)transformedIndices.get("atc"));
		
		assertTrue(transformedIndices.containsKey("Anzahl_Packungen".toLowerCase()));
		assertEquals(2, (int)transformedIndices.get("Anzahl_Packungen".toLowerCase()));
		
		assertTrue(transformedIndices.containsKey("Verordnungsdatum".toLowerCase()));
		assertEquals(3, (int)transformedIndices.get("Verordnungsdatum".toLowerCase()));
		
		assertTrue(transformedIndices.containsKey(WidoColumn.DDDPK.getLabel()));
		assertEquals(4, (int)transformedIndices.get(WidoColumn.DDDPK.getLabel()));

		assertTrue(transformedIndices.containsKey(WidoColumn.APPFORM.getLabel()));
		assertEquals(5, (int)transformedIndices.get(WidoColumn.APPFORM.getLabel()));
		
		assertTrue(transformedIndices.containsKey(WidoColumn.GENERIC.getLabel()));
		assertEquals(6, (int)transformedIndices.get(WidoColumn.GENERIC.getLabel()));
		
		List<RowElement> transformedColumns = transformedRow.getColumns();
		assertNotNull(transformedColumns);
		assertFalse(transformedColumns.isEmpty());
		assertEquals(transformedIndices.size(), transformedColumns.size());
		
		RowElement pzn = transformedColumns.get(0);
		assertNotNull(pzn);
		assertEquals("00000002", pzn.getContent());
		
		RowElement atc = transformedColumns.get(1);
		assertNotNull(atc);
		assertEquals("abcde", atc.getContent());
		
		RowElement packages = transformedColumns.get(2);
		assertNotNull(packages);
		assertEquals("10", packages.getContent());
		
		RowElement prescriptionDate = transformedColumns.get(3);
		assertNotNull(prescriptionDate);
		assertEquals("2010-04-01", prescriptionDate.getContent());
		
		RowElement ddd = transformedColumns.get(4);
		assertNotNull(ddd);
		assertEquals("200.0", ddd.getContent());
		
		RowElement appForm = transformedColumns.get(5);
		assertNotNull(appForm);
		assertEquals("1", appForm.getContent());
		
		RowElement generic = transformedColumns.get(6);
		assertNotNull(generic);
		assertEquals("", generic.getContent());
	}
	
	@Test
	public void padToExpectedLength() throws JsonParseException, JsonMappingException, IOException {
		AppendConfiguration config = new ObjectMapper().readValue(new File("src/test/resources/configuration/etl/dynamicAppending/DDD_without_name.json"), AppendConfiguration.class);
		Transformer transformer = DDDTransformer.of(config.getTargetDb(), config.getTargetTable(), config.getKeyColumn(), config.getSources());
		List<RowElement> columns = Arrays.asList(
				new SimpleRowElement("00000005", TeradataColumnType.CHARACTER),
				new SimpleRowElement("abcde", TeradataColumnType.CHARACTER),
				new SimpleRowElement("10", TeradataColumnType.CHARACTER),
				new SimpleRowElement("2010-04-01", TeradataColumnType.CHARACTER)
		);
		Map<String,Integer> columnNames2Index = new HashMap<>(3);
		columnNames2Index.put("pzn", 0);
		columnNames2Index.put("atc", 1);
		columnNames2Index.put("Anzahl_Packungen".toLowerCase(), 2);
		columnNames2Index.put("Verordnungsdatum".toLowerCase(), 3);
		Row row = new Row(config.getTargetDb(), config.getTargetTable(), columns, columnNames2Index);
		
		Row transformedRow = transformer.transform(row);
		assertNotNull(transformedRow);
		assertEquals(row.getDb(), transformedRow.getDb());
		assertEquals(row.getTable(), transformedRow.getTable());
		
		Map<String,Integer> transformedIndices = transformedRow.getColumnName2Index();
		assertNotNull(transformedIndices);
		assertFalse(transformedIndices.isEmpty());
		
		assertTrue(transformedIndices.containsKey("pzn"));
		assertEquals(0, (int)transformedIndices.get("pzn"));
		
		assertTrue(transformedIndices.containsKey("atc"));
		assertEquals(1, (int)transformedIndices.get("atc"));
		
		assertTrue(transformedIndices.containsKey("Anzahl_Packungen".toLowerCase()));
		assertEquals(2, (int)transformedIndices.get("Anzahl_Packungen".toLowerCase()));
		
		assertTrue(transformedIndices.containsKey("Verordnungsdatum".toLowerCase()));
		assertEquals(3, (int)transformedIndices.get("Verordnungsdatum".toLowerCase()));
		
		assertTrue(transformedIndices.containsKey(WidoColumn.DDDPK.getLabel()));
		assertEquals(4, (int)transformedIndices.get(WidoColumn.DDDPK.getLabel()));

		assertTrue(transformedIndices.containsKey(WidoColumn.APPFORM.getLabel()));
		assertEquals(5, (int)transformedIndices.get(WidoColumn.APPFORM.getLabel()));
		
		assertTrue(transformedIndices.containsKey(WidoColumn.GENERIC.getLabel()));
		assertEquals(6, (int)transformedIndices.get(WidoColumn.GENERIC.getLabel()));
		
		List<RowElement> transformedColumns = transformedRow.getColumns();
		assertNotNull(transformedColumns);
		assertFalse(transformedColumns.isEmpty());
		assertEquals(transformedIndices.size(), transformedColumns.size());
		
		RowElement pzn = transformedColumns.get(0);
		assertNotNull(pzn);
		assertEquals("00000005", pzn.getContent());
		
		RowElement atc = transformedColumns.get(1);
		assertNotNull(atc);
		assertEquals("abcde", atc.getContent());
		
		RowElement packages = transformedColumns.get(2);
		assertNotNull(packages);
		assertEquals("10", packages.getContent());
		
		RowElement prescriptionDate = transformedColumns.get(3);
		assertNotNull(prescriptionDate);
		assertEquals("2010-04-01", prescriptionDate.getContent());
		
		RowElement ddd = transformedColumns.get(4);
		assertNotNull(ddd);
		assertEquals("", ddd.getContent());
		
		RowElement appForm = transformedColumns.get(5);
		assertNotNull(appForm);
		assertEquals("", appForm.getContent());
		
		RowElement generic = transformedColumns.get(6);
		assertNotNull(generic);
		assertEquals("", generic.getContent());
	}
	
	@Test
	public void skipProcessingIfDatabaseDoesNotMatch() throws JsonParseException, JsonMappingException, IOException {
		AppendConfiguration config = new ObjectMapper().readValue(new File("src/test/resources/configuration/etl/dynamicAppending/DDD_without_name.json"), AppendConfiguration.class);
		Transformer transformer = DDDTransformer.of(config.getTargetDb(), config.getTargetTable(), config.getKeyColumn(), config.getSources());
		List<RowElement> columns = Arrays.asList(
				new SimpleRowElement("00000005", TeradataColumnType.CHARACTER),
				new SimpleRowElement("abcde", TeradataColumnType.CHARACTER),
				new SimpleRowElement("10", TeradataColumnType.CHARACTER)
		);
		Map<String,Integer> columnNames2Index = new HashMap<>(3);
		columnNames2Index.put("pzn", 0);
		columnNames2Index.put("atc", 1);
		columnNames2Index.put("Anzahl_Packungen".toLowerCase(), 2);

		Row row = new Row("some db", config.getTargetTable(), columns, columnNames2Index);
		
		Row transformedRow = transformer.transform(row);
		assertNotNull(transformedRow);
		assertEquals("some db", transformedRow.getDb());
		assertEquals(row.getTable(), transformedRow.getTable());
		
		Map<String,Integer> transformedIndices = transformedRow.getColumnName2Index();
		assertNotNull(transformedIndices);
		assertFalse(transformedIndices.isEmpty());
		
		assertTrue(transformedIndices.containsKey("pzn"));
		assertEquals(0, (int)transformedIndices.get("pzn"));
		
		assertTrue(transformedIndices.containsKey("atc"));
		assertEquals(1, (int)transformedIndices.get("atc"));
		
		assertTrue(transformedIndices.containsKey("Anzahl_Packungen".toLowerCase()));
		assertEquals(2, (int)transformedIndices.get("Anzahl_Packungen".toLowerCase()));
		
		List<RowElement> transformedColumns = transformedRow.getColumns();
		assertNotNull(transformedColumns);
		assertFalse(transformedColumns.isEmpty());
		assertEquals(transformedIndices.size(), transformedColumns.size());
		
		RowElement pzn = transformedColumns.get(0);
		assertNotNull(pzn);
		assertEquals("00000005", pzn.getContent());
		
		RowElement atc = transformedColumns.get(1);
		assertNotNull(atc);
		assertEquals("abcde", atc.getContent());
		
		RowElement packages = transformedColumns.get(2);
		assertNotNull(packages);
		assertEquals("10", packages.getContent());
	}
	
	@Test
	public void skipProcessingIfTableDoesNotMatch() throws JsonParseException, JsonMappingException, IOException {
		AppendConfiguration config = new ObjectMapper().readValue(new File("src/test/resources/configuration/etl/dynamicAppending/DDD_without_name.json"), AppendConfiguration.class);
		Transformer transformer = DDDTransformer.of(config.getTargetDb(), config.getTargetTable(), config.getKeyColumn(), config.getSources());
		List<RowElement> columns = Arrays.asList(
				new SimpleRowElement("00000005", TeradataColumnType.CHARACTER),
				new SimpleRowElement("abcde", TeradataColumnType.CHARACTER),
				new SimpleRowElement("10", TeradataColumnType.CHARACTER)
		);
		Map<String,Integer> columnNames2Index = new HashMap<>(3);
		columnNames2Index.put("pzn", 0);
		columnNames2Index.put("atc", 1);
		columnNames2Index.put("Anzahl_Packungen".toLowerCase(), 2);

		Row row = new Row(config.getTargetDb(), "some table", columns, columnNames2Index);
		
		Row transformedRow = transformer.transform(row);
		assertNotNull(transformedRow);
		assertEquals(row.getDb(), transformedRow.getDb());
		assertEquals("some table", transformedRow.getTable());
		
		Map<String,Integer> transformedIndices = transformedRow.getColumnName2Index();
		assertNotNull(transformedIndices);
		assertFalse(transformedIndices.isEmpty());
		
		assertTrue(transformedIndices.containsKey("pzn"));
		assertEquals(0, (int)transformedIndices.get("pzn"));
		
		assertTrue(transformedIndices.containsKey("atc"));
		assertEquals(1, (int)transformedIndices.get("atc"));
		
		assertTrue(transformedIndices.containsKey("Anzahl_Packungen".toLowerCase()));
		assertEquals(2, (int)transformedIndices.get("Anzahl_Packungen".toLowerCase()));
		
		List<RowElement> transformedColumns = transformedRow.getColumns();
		assertNotNull(transformedColumns);
		assertFalse(transformedColumns.isEmpty());
		assertEquals(transformedIndices.size(), transformedColumns.size());
		
		RowElement pzn = transformedColumns.get(0);
		assertNotNull(pzn);
		assertEquals("00000005", pzn.getContent());
		
		RowElement atc = transformedColumns.get(1);
		assertNotNull(atc);
		assertEquals("abcde", atc.getContent());
		
		RowElement packages = transformedColumns.get(2);
		assertNotNull(packages);
		assertEquals("10", packages.getContent());
	}
	
	@Test
	public void setDDDToZeroIfPrescriptionDateIsNotInValidityDateRange() throws IOException {
		AppendConfiguration config = new ObjectMapper().readValue(new File("src/test/resources/configuration/etl/dynamicAppending/DDD_without_name.json"), AppendConfiguration.class);
		Transformer transformer = DDDTransformer.of(config.getTargetDb(), config.getTargetTable(), config.getKeyColumn(), config.getSources());
		List<RowElement> columns = Arrays.asList(
				new SimpleRowElement("00000006", TeradataColumnType.CHARACTER),
				new SimpleRowElement("abcde", TeradataColumnType.CHARACTER),
				new SimpleRowElement("10", TeradataColumnType.CHARACTER),
				new SimpleRowElement("2011-04-01", TeradataColumnType.CHARACTER)
		);
		Map<String,Integer> columnNames2Index = new HashMap<>(3);
		columnNames2Index.put("pzn", 0);
		columnNames2Index.put("atc", 1);
		columnNames2Index.put("Anzahl_Packungen".toLowerCase(), 2);
		columnNames2Index.put("Verordnungsdatum".toLowerCase(), 3);
		Row row = new Row(config.getTargetDb(), config.getTargetTable(), columns, columnNames2Index);
		
		Row transformedRow = transformer.transform(row);
		assertNotNull(transformedRow);
		assertEquals(row.getDb(), transformedRow.getDb());
		assertEquals(row.getTable(), transformedRow.getTable());
		
		Map<String,Integer> transformedIndices = transformedRow.getColumnName2Index();
		assertNotNull(transformedIndices);
		assertFalse(transformedIndices.isEmpty());
		
		assertTrue(transformedIndices.containsKey("pzn"));
		assertEquals(0, (int)transformedIndices.get("pzn"));
		
		assertTrue(transformedIndices.containsKey("atc"));
		assertEquals(1, (int)transformedIndices.get("atc"));
		
		assertTrue(transformedIndices.containsKey("Anzahl_Packungen".toLowerCase()));
		assertEquals(2, (int)transformedIndices.get("Anzahl_Packungen".toLowerCase()));
		
		assertTrue(transformedIndices.containsKey("Verordnungsdatum".toLowerCase()));
		assertEquals(3, (int)transformedIndices.get("Verordnungsdatum".toLowerCase()));
		
		assertTrue(transformedIndices.containsKey(WidoColumn.DDDPK.getLabel()));
		assertEquals(4, (int)transformedIndices.get(WidoColumn.DDDPK.getLabel()));

		assertTrue(transformedIndices.containsKey(WidoColumn.APPFORM.getLabel()));
		assertEquals(5, (int)transformedIndices.get(WidoColumn.APPFORM.getLabel()));
		
		assertTrue(transformedIndices.containsKey(WidoColumn.GENERIC.getLabel()));
		assertEquals(6, (int)transformedIndices.get(WidoColumn.GENERIC.getLabel()));
		
		List<RowElement> transformedColumns = transformedRow.getColumns();
		assertNotNull(transformedColumns);
		assertFalse(transformedColumns.isEmpty());
		assertEquals(transformedIndices.size(), transformedColumns.size());
		
		RowElement pzn = transformedColumns.get(0);
		assertNotNull(pzn);
		assertEquals("00000006", pzn.getContent());
		
		RowElement atc = transformedColumns.get(1);
		assertNotNull(atc);
		assertEquals("abcde", atc.getContent());
		
		RowElement packages = transformedColumns.get(2);
		assertNotNull(packages);
		assertEquals("10", packages.getContent());
		
		RowElement prescriptionDate = transformedColumns.get(3);
		assertNotNull(prescriptionDate);
		assertEquals("2011-04-01", prescriptionDate.getContent());
		
		RowElement ddd = transformedColumns.get(4);
		assertNotNull(ddd);
		assertEquals("0.0", ddd.getContent());
		
		RowElement appForm = transformedColumns.get(5);
		assertNotNull(appForm);
		assertEquals("3", appForm.getContent());
		
		RowElement generic = transformedColumns.get(6);
		assertNotNull(generic);
		assertEquals("", generic.getContent());
	}
	
	@Test
	public void setDDDToZeroIfPrescriptionDateIsLargerThanEndDateAndStartDateIsMissing() throws IOException {
		AppendConfiguration config = new ObjectMapper().readValue(new File("src/test/resources/configuration/etl/dynamicAppending/DDD_without_name.json"), AppendConfiguration.class);
		Transformer transformer = DDDTransformer.of(config.getTargetDb(), config.getTargetTable(), config.getKeyColumn(), config.getSources());
		List<RowElement> columns = Arrays.asList(
				new SimpleRowElement("00000007", TeradataColumnType.CHARACTER),
				new SimpleRowElement("abcde", TeradataColumnType.CHARACTER),
				new SimpleRowElement("10", TeradataColumnType.CHARACTER),
				new SimpleRowElement("2011-04-01", TeradataColumnType.CHARACTER)
		);
		Map<String,Integer> columnNames2Index = new HashMap<>(3);
		columnNames2Index.put("pzn", 0);
		columnNames2Index.put("atc", 1);
		columnNames2Index.put("Anzahl_Packungen".toLowerCase(), 2);
		columnNames2Index.put("Verordnungsdatum".toLowerCase(), 3);
		
		Row row = new Row(config.getTargetDb(), config.getTargetTable(), columns, columnNames2Index);
		
		Row transformedRow = transformer.transform(row);
		assertNotNull(transformedRow);
		assertEquals(row.getDb(), transformedRow.getDb());
		assertEquals(row.getTable(), transformedRow.getTable());
		
		Map<String,Integer> transformedIndices = transformedRow.getColumnName2Index();
		assertNotNull(transformedIndices);
		assertFalse(transformedIndices.isEmpty());
		
		assertTrue(transformedIndices.containsKey("pzn"));
		assertEquals(0, (int)transformedIndices.get("pzn"));
		
		assertTrue(transformedIndices.containsKey("atc"));
		assertEquals(1, (int)transformedIndices.get("atc"));
		
		assertTrue(transformedIndices.containsKey("Anzahl_Packungen".toLowerCase()));
		assertEquals(2, (int)transformedIndices.get("Anzahl_Packungen".toLowerCase()));
		
		assertTrue(transformedIndices.containsKey("Verordnungsdatum".toLowerCase()));
		assertEquals(3, (int)transformedIndices.get("Verordnungsdatum".toLowerCase()));
		
		assertTrue(transformedIndices.containsKey(WidoColumn.DDDPK.getLabel()));
		assertEquals(4, (int)transformedIndices.get(WidoColumn.DDDPK.getLabel()));

		assertTrue(transformedIndices.containsKey(WidoColumn.APPFORM.getLabel()));
		assertEquals(5, (int)transformedIndices.get(WidoColumn.APPFORM.getLabel()));
		
		assertTrue(transformedIndices.containsKey(WidoColumn.GENERIC.getLabel()));
		assertEquals(6, (int)transformedIndices.get(WidoColumn.GENERIC.getLabel()));
		
		List<RowElement> transformedColumns = transformedRow.getColumns();
		assertNotNull(transformedColumns);
		assertFalse(transformedColumns.isEmpty());
		assertEquals(transformedIndices.size(), transformedColumns.size());
		
		RowElement pzn = transformedColumns.get(0);
		assertNotNull(pzn);
		assertEquals("00000007", pzn.getContent());
		
		RowElement atc = transformedColumns.get(1);
		assertNotNull(atc);
		assertEquals("abcde", atc.getContent());
		
		RowElement packages = transformedColumns.get(2);
		assertNotNull(packages);
		assertEquals("10", packages.getContent());
		
		RowElement prescriptionDate = transformedColumns.get(3);
		assertNotNull(prescriptionDate);
		assertEquals("2011-04-01", prescriptionDate.getContent());
		
		RowElement ddd = transformedColumns.get(4);
		assertNotNull(ddd);
		assertEquals("0.0", ddd.getContent());
		
		RowElement appForm = transformedColumns.get(5);
		assertNotNull(appForm);
		assertEquals("4", appForm.getContent());
		
		RowElement generic = transformedColumns.get(6);
		assertNotNull(generic);
		assertEquals("", generic.getContent());
	}
	
	@Test
	public void setDDDToZeroIfDateRangeIsMissing() throws IOException {
		AppendConfiguration config = new ObjectMapper().readValue(new File("src/test/resources/configuration/etl/dynamicAppending/DDD_without_name.json"), AppendConfiguration.class);
		Transformer transformer = DDDTransformer.of(config.getTargetDb(), config.getTargetTable(), config.getKeyColumn(), config.getSources());
		List<RowElement> columns = Arrays.asList(
				new SimpleRowElement("00000008", TeradataColumnType.CHARACTER),
				new SimpleRowElement("abcde", TeradataColumnType.CHARACTER),
				new SimpleRowElement("10", TeradataColumnType.CHARACTER),
				new SimpleRowElement("2011-04-01", TeradataColumnType.CHARACTER)
		);
		Map<String,Integer> columnNames2Index = new HashMap<>(3);
		columnNames2Index.put("pzn", 0);
		columnNames2Index.put("atc", 1);
		columnNames2Index.put("Anzahl_Packungen".toLowerCase(), 2);
		columnNames2Index.put("Verordnungsdatum".toLowerCase(), 3);
		
		Row row = new Row(config.getTargetDb(), config.getTargetTable(), columns, columnNames2Index);
		
		Row transformedRow = transformer.transform(row);
		assertNotNull(transformedRow);
		assertEquals(row.getDb(), transformedRow.getDb());
		assertEquals(row.getTable(), transformedRow.getTable());
		
		Map<String,Integer> transformedIndices = transformedRow.getColumnName2Index();
		assertNotNull(transformedIndices);
		assertFalse(transformedIndices.isEmpty());
		
		assertTrue(transformedIndices.containsKey("pzn"));
		assertEquals(0, (int)transformedIndices.get("pzn"));
		
		assertTrue(transformedIndices.containsKey("atc"));
		assertEquals(1, (int)transformedIndices.get("atc"));
		
		assertTrue(transformedIndices.containsKey("Anzahl_Packungen".toLowerCase()));
		assertEquals(2, (int)transformedIndices.get("Anzahl_Packungen".toLowerCase()));
		
		assertTrue(transformedIndices.containsKey("Verordnungsdatum".toLowerCase()));
		assertEquals(3, (int)transformedIndices.get("Verordnungsdatum".toLowerCase()));
		
		assertTrue(transformedIndices.containsKey(WidoColumn.DDDPK.getLabel()));
		assertEquals(4, (int)transformedIndices.get(WidoColumn.DDDPK.getLabel()));

		assertTrue(transformedIndices.containsKey(WidoColumn.APPFORM.getLabel()));
		assertEquals(5, (int)transformedIndices.get(WidoColumn.APPFORM.getLabel()));
		
		assertTrue(transformedIndices.containsKey(WidoColumn.GENERIC.getLabel()));
		assertEquals(6, (int)transformedIndices.get(WidoColumn.GENERIC.getLabel()));
		
		List<RowElement> transformedColumns = transformedRow.getColumns();
		assertNotNull(transformedColumns);
		assertFalse(transformedColumns.isEmpty());
		assertEquals(transformedIndices.size(), transformedColumns.size());
		
		RowElement pzn = transformedColumns.get(0);
		assertNotNull(pzn);
		assertEquals("00000008", pzn.getContent());
		
		RowElement atc = transformedColumns.get(1);
		assertNotNull(atc);
		assertEquals("abcde", atc.getContent());
		
		RowElement packages = transformedColumns.get(2);
		assertNotNull(packages);
		assertEquals("10", packages.getContent());
		
		RowElement prescriptionDate = transformedColumns.get(3);
		assertNotNull(prescriptionDate);
		assertEquals("2011-04-01", prescriptionDate.getContent());
		
		RowElement ddd = transformedColumns.get(4);
		assertNotNull(ddd);
		assertEquals("0.0", ddd.getContent());
		
		RowElement appForm = transformedColumns.get(5);
		assertNotNull(appForm);
		assertEquals("5", appForm.getContent());
		
		RowElement generic = transformedColumns.get(6);
		assertNotNull(generic);
		assertEquals("", generic.getContent());
	}
	
	@Test
	public void addClassificationFlagWhenNameIsConsidered() throws IOException {
		AppendConfiguration config = new ObjectMapper().readValue(new File("src/test/resources/configuration/etl/dynamicAppending/DDD_with_name.json"), AppendConfiguration.class);
		Transformer transformer = DDDTransformer.of(config.getTargetDb(), config.getTargetTable(), config.getKeyColumn(), config.getSources());
		List<RowElement> columns = Arrays.asList(
				new SimpleRowElement("00000009", TeradataColumnType.CHARACTER),
				new SimpleRowElement("abcde", TeradataColumnType.CHARACTER),
				new SimpleRowElement("10", TeradataColumnType.CHARACTER),
				new SimpleRowElement("2011-04-01", TeradataColumnType.CHARACTER)
		);
		Map<String,Integer> columnNames2Index = new HashMap<>(4);
		columnNames2Index.put("pzn", 0);
		columnNames2Index.put("atc", 1);
		columnNames2Index.put("Anzahl_Packungen".toLowerCase(), 2);
		columnNames2Index.put("Verordnungsdatum".toLowerCase(), 3);
		
		Row row = new Row(config.getTargetDb(), config.getTargetTable(), columns, columnNames2Index);
		
		Row transformedRow = transformer.transform(row);
		assertNotNull(transformedRow);
		assertEquals(row.getDb(), transformedRow.getDb());
		assertEquals(row.getTable(), transformedRow.getTable());
		
		Map<String,Integer> transformedIndices = transformedRow.getColumnName2Index();
		assertNotNull(transformedIndices);
		assertFalse(transformedIndices.isEmpty());
		
		assertTrue(transformedIndices.containsKey("pzn"));
		assertEquals(0, (int)transformedIndices.get("pzn"));
		
		assertTrue(transformedIndices.containsKey("atc"));
		assertEquals(1, (int)transformedIndices.get("atc"));
		
		assertTrue(transformedIndices.containsKey("Anzahl_Packungen".toLowerCase()));
		assertEquals(2, (int)transformedIndices.get("Anzahl_Packungen".toLowerCase()));
		
		assertTrue(transformedIndices.containsKey("Verordnungsdatum".toLowerCase()));
		assertEquals(3, (int)transformedIndices.get("Verordnungsdatum".toLowerCase()));
		
		assertTrue(transformedIndices.containsKey(WidoColumn.DDDPK.getLabel()));
		assertEquals(4, (int)transformedIndices.get(WidoColumn.DDDPK.getLabel()));

		assertTrue(transformedIndices.containsKey(WidoColumn.APPFORM.getLabel()));
		assertEquals(5, (int)transformedIndices.get(WidoColumn.APPFORM.getLabel()));
		
		assertTrue(transformedIndices.containsKey(WidoColumn.STANAME.getLabel()));
		assertEquals(6, (int)transformedIndices.get(WidoColumn.STANAME.getLabel()));
		
		assertTrue(transformedIndices.containsKey(WidoColumn.GENERIC.getLabel()));
		assertEquals(7, (int)transformedIndices.get(WidoColumn.GENERIC.getLabel()));
			
		assertTrue(transformedIndices.containsKey("IS_CLASSIFIED"));
		assertEquals(8, (int)transformedIndices.get("IS_CLASSIFIED"));
		
		List<RowElement> transformedColumns = transformedRow.getColumns();
		assertNotNull(transformedColumns);
		assertFalse(transformedColumns.isEmpty());
		assertEquals(transformedIndices.size(), transformedColumns.size());
		
		RowElement pzn = transformedColumns.get(0);
		assertNotNull(pzn);
		assertEquals("00000009", pzn.getContent());
		
		RowElement atc = transformedColumns.get(1);
		assertNotNull(atc);
		assertEquals("abcde", atc.getContent());
		
		RowElement packages = transformedColumns.get(2);
		assertNotNull(packages);
		assertEquals("10", packages.getContent());
		
		RowElement prescriptionDate = transformedColumns.get(3);
		assertNotNull(prescriptionDate);
		assertEquals("2011-04-01", prescriptionDate.getContent());
		
		RowElement ddd = transformedColumns.get(4);
		assertNotNull(ddd);
		assertEquals("900.0", ddd.getContent());
		
		RowElement appForm = transformedColumns.get(5);
		assertNotNull(appForm);
		assertEquals("6", appForm.getContent());
		
		RowElement name = transformedColumns.get(6);
		assertNotNull(name);
		assertEquals("Neun", name.getContent());
		
		RowElement generic = transformedColumns.get(7);
		assertNotNull(generic);
		assertEquals("", generic.getContent());
				
		RowElement classificationFlag = transformedColumns.get(8);
		assertNotNull(classificationFlag);
		assertEquals("1", classificationFlag.getContent());
	}
	
	@Test
	public void setClassificationFlagToFalseWhenUnclassifiedPackage() throws IOException {
		AppendConfiguration config = new ObjectMapper().readValue(new File("src/test/resources/configuration/etl/dynamicAppending/DDD_with_name.json"), AppendConfiguration.class);
		Transformer transformer = DDDTransformer.of(config.getTargetDb(), config.getTargetTable(), config.getKeyColumn(), config.getSources());
		List<RowElement> columns = Arrays.asList(
				new SimpleRowElement("00000010", TeradataColumnType.CHARACTER),
				new SimpleRowElement("abcde", TeradataColumnType.CHARACTER),
				new SimpleRowElement("10", TeradataColumnType.CHARACTER),
				new SimpleRowElement("2011-04-01", TeradataColumnType.CHARACTER)
		);
		Map<String,Integer> columnNames2Index = new HashMap<>(4);
		columnNames2Index.put("pzn", 0);
		columnNames2Index.put("atc", 1);
		columnNames2Index.put("Anzahl_Packungen".toLowerCase(), 2);
		columnNames2Index.put("Verordnungsdatum".toLowerCase(), 3);
		
		Row row = new Row(config.getTargetDb(), config.getTargetTable(), columns, columnNames2Index);
		
		Row transformedRow = transformer.transform(row);
		assertNotNull(transformedRow);
		assertEquals(row.getDb(), transformedRow.getDb());
		assertEquals(row.getTable(), transformedRow.getTable());
		
		Map<String,Integer> transformedIndices = transformedRow.getColumnName2Index();
		assertNotNull(transformedIndices);
		assertFalse(transformedIndices.isEmpty());
		
		assertTrue(transformedIndices.containsKey("pzn"));
		assertEquals(0, (int)transformedIndices.get("pzn"));
		
		assertTrue(transformedIndices.containsKey("atc"));
		assertEquals(1, (int)transformedIndices.get("atc"));
		
		assertTrue(transformedIndices.containsKey("Anzahl_Packungen".toLowerCase()));
		assertEquals(2, (int)transformedIndices.get("Anzahl_Packungen".toLowerCase()));
		
		assertTrue(transformedIndices.containsKey("Verordnungsdatum".toLowerCase()));
		assertEquals(3, (int)transformedIndices.get("Verordnungsdatum".toLowerCase()));
		
		assertTrue(transformedIndices.containsKey(WidoColumn.DDDPK.getLabel()));
		assertEquals(4, (int)transformedIndices.get(WidoColumn.DDDPK.getLabel()));

		assertTrue(transformedIndices.containsKey(WidoColumn.APPFORM.getLabel()));
		assertEquals(5, (int)transformedIndices.get(WidoColumn.APPFORM.getLabel()));
		
		assertTrue(transformedIndices.containsKey(WidoColumn.STANAME.getLabel()));
		assertEquals(6, (int)transformedIndices.get(WidoColumn.STANAME.getLabel()));

		assertTrue(transformedIndices.containsKey(WidoColumn.GENERIC.getLabel()));
		assertEquals(7, (int)transformedIndices.get(WidoColumn.GENERIC.getLabel()));
				
		assertTrue(transformedIndices.containsKey("IS_CLASSIFIED"));
		assertEquals(8, (int)transformedIndices.get("IS_CLASSIFIED"));
		
		List<RowElement> transformedColumns = transformedRow.getColumns();
		assertNotNull(transformedColumns);
		assertFalse(transformedColumns.isEmpty());
		assertEquals(transformedIndices.size(), transformedColumns.size());
		
		RowElement pzn = transformedColumns.get(0);
		assertNotNull(pzn);
		assertEquals("00000010", pzn.getContent());
		
		RowElement atc = transformedColumns.get(1);
		assertNotNull(atc);
		assertEquals("abcde", atc.getContent());
		
		RowElement packages = transformedColumns.get(2);
		assertNotNull(packages);
		assertEquals("10", packages.getContent());
		
		RowElement prescriptionDate = transformedColumns.get(3);
		assertNotNull(prescriptionDate);
		assertEquals("2011-04-01", prescriptionDate.getContent());
		
		RowElement ddd = transformedColumns.get(4);
		assertNotNull(ddd);
		assertEquals("1000.0", ddd.getContent());
		
		RowElement appForm = transformedColumns.get(5);
		assertNotNull(appForm);
		assertEquals("7", appForm.getContent());
		
		RowElement name = transformedColumns.get(6);
		assertNotNull(name);
		assertEquals("Zehn", name.getContent());

		RowElement generic = transformedColumns.get(7);
		assertNotNull(generic);
		assertEquals("", generic.getContent());
				
		RowElement classificationFlag = transformedColumns.get(8);
		assertNotNull(classificationFlag);
		assertEquals("0", classificationFlag.getContent());
	}
}
