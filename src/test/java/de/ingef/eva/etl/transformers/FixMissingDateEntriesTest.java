package de.ingef.eva.etl.transformers;

import de.ingef.eva.data.RowElement;
import de.ingef.eva.data.SimpleRowElement;
import de.ingef.eva.data.TeradataColumnType;
import de.ingef.eva.etl.Row;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class FixMissingDateEntriesTest {

    @Test
    public void ignoreRowIfContractIsNotKV() {
        List<RowElement> columns = new ArrayList<>(3);
        columns.add(new SimpleRowElement("", TeradataColumnType.VARCHAR));
        columns.add(new SimpleRowElement("1", TeradataColumnType.VARCHAR));
        columns.add(new SimpleRowElement("", TeradataColumnType.VARCHAR));
        Map<String, Integer> columnIndices = new HashMap<>(3);
        columnIndices.put("vertrags_id", 0);
        columnIndices.put("behandl_quartal", 1);
        columnIndices.put("behandl_beginn", 2);
        Row row = new Row("ADB", "Arzt_Fall", columns, columnIndices);

        Row transformedRow = new FixMissingDateEntries().transform(row);
        assertNotNull(transformedRow);

        List<RowElement> transformedColumns = transformedRow.getColumns();
        assertNotNull(transformedColumns);
        assertEquals(columns.size(), transformedColumns.size());
        assertEquals(columns.get(0), transformedColumns.get(0));
        assertEquals(columns.get(1), transformedColumns.get(1));
        assertEquals(columns.get(2), transformedColumns.get(2));

        Map<String, Integer> transformedIndices = transformedRow.getColumnName2Index();
        assertNotNull(transformedIndices);
        assertTrue(transformedIndices.containsKey("vertrags_id"));
        assertEquals(columnIndices.get("vertrags_id"), transformedIndices.get("vertrags_id"));
        assertTrue(transformedIndices.containsKey("behandl_quartal"));
        assertEquals(columnIndices.get("behandl_quartal"), transformedIndices.get("behandl_quartal"));
        assertTrue(transformedIndices.containsKey("behandl_beginn"));
        assertEquals(columnIndices.get("behandl_beginn"), transformedIndices.get("behandl_beginn"));
    }

    @Test
    public void ignoreRowIfDateIsSet() {
        List<RowElement> columns = new ArrayList<>(3);
        columns.add(new SimpleRowElement("KV", TeradataColumnType.VARCHAR));
        columns.add(new SimpleRowElement("1", TeradataColumnType.VARCHAR));
        columns.add(new SimpleRowElement("20100101", TeradataColumnType.VARCHAR));
        columns.add(new SimpleRowElement("20100331", TeradataColumnType.VARCHAR));
        Map<String, Integer> columnIndices = new HashMap<>(3);
        columnIndices.put("vertrags_id", 0);
        columnIndices.put("behandl_quartal", 1);
        columnIndices.put("behandl_beginn", 2);
        columnIndices.put("behandl_ende", 3);
        Row row = new Row("ADB", "Arzt_Fall", columns, columnIndices);

        Row transformedRow = new FixMissingDateEntries().transform(row);
        assertNotNull(transformedRow);

        List<RowElement> transformedColumns = transformedRow.getColumns();
        assertNotNull(transformedColumns);
        assertEquals(columns.size(), transformedColumns.size());
        assertEquals(columns.get(0), transformedColumns.get(0));
        assertEquals(columns.get(1), transformedColumns.get(1));
        assertEquals(columns.get(2), transformedColumns.get(2));
        assertEquals(columns.get(3), transformedColumns.get(3));

        Map<String, Integer> transformedIndices = transformedRow.getColumnName2Index();
        assertNotNull(transformedIndices);
        assertTrue(transformedIndices.containsKey("vertrags_id"));
        assertEquals(columnIndices.get("vertrags_id"), transformedIndices.get("vertrags_id"));
        assertTrue(transformedIndices.containsKey("behandl_quartal"));
        assertEquals(columnIndices.get("behandl_quartal"), transformedIndices.get("behandl_quartal"));
        assertTrue(transformedIndices.containsKey("behandl_beginn"));
        assertEquals(columnIndices.get("behandl_beginn"), transformedIndices.get("behandl_beginn"));
        assertTrue(transformedIndices.containsKey("behandl_ende"));
        assertEquals(columnIndices.get("behandl_ende"), transformedIndices.get("behandl_ende"));

    }

    @Test
    public void useQ1LimitsWhenTreatmentQuarterIsQ1() {
        testBothDateModification("1", "20100101", "20100331");
    }

    @Test
    public void useQ1LowerLimitWhenTreatmentQuarterIsQ1AndStartDateIsMissing() {
        testStartDateModification("1", "20100101");
    }

    @Test
    public void useQ1LowerLimitWhenTreatmentQuarterIsQ1AndEndDateIsMissing() {
        testEndDateModification("1", "20100331");
    }

    @Test
    public void useQ2LimitsWhenTreatmentQuarterIsQ2() {
        testBothDateModification("2", "20100401", "20100630");
    }

    @Test
    public void useQ2LowerLimitWhenTreatmentQuarterIsQ2AndStartDateIsMissing() {
        testStartDateModification("2", "20100401");
    }

    @Test
    public void useQ2LowerLimitWhenTreatmentQuarterIsQ2AndEndDateIsMissing() {
        testEndDateModification("2", "20100630");
    }

    @Test
    public void useQ3LimitsWhenTreatmentQuarterIsQ3() {
        testBothDateModification("3", "20100701", "20100930");
    }

    @Test
    public void useQ3LowerLimitWhenTreatmentQuarterIsQ3AndStartDateIsMissing() {
        testStartDateModification("3", "20100701");
    }

    @Test
    public void useQ3LowerLimitWhenTreatmentQuarterIsQ3AndEndDateIsMissing() {
        testEndDateModification("3", "20100930");
    }

    @Test
    public void useQ4LimitsWhenTreatmentQuarterIsQ4() {
        testBothDateModification("4", "20101001", "20101231");
    }

    @Test
    public void useQ4LowerLimitWhenTreatmentQuarterIsQ4AndStartDateIsMissing() {
        testStartDateModification("4", "20101001");
    }

    @Test
    public void useQ4LowerLimitWhenTreatmentQuarterIsQ4AndEndDateIsMissing() {
        testEndDateModification("4", "20101231");
    }

    private void testBothDateModification(String startQuarter, String expectedStartDate, String expectedEndDate) {
        Row row = createRowWithoutDates(startQuarter);
        Map<String, Integer> indices = row.getColumnName2Index();
        List<RowElement> columns = row.getColumns();
        Row transformedRow = new FixMissingDateEntries().transform(row);
        assertNotNull(transformedRow);

        Map<String, Integer> transformedIndices = transformedRow.getColumnName2Index();
        assertNotNull(transformedIndices);
        transformedIndices.containsKey("vertrags_id");
        assertEquals(indices.get("vertrags_id"), transformedIndices.get("vertrags_id"));
        transformedIndices.containsKey("behandl_quartal");
        assertEquals(indices.get("behandl_quartal"), transformedIndices.get("behandl_quartal"));
        transformedIndices.containsKey("behandl_beginn");
        assertEquals(indices.get("behandl_beginn"), transformedIndices.get("behandl_beginn"));
        transformedIndices.containsKey("behandl_ende");
        assertEquals(indices.get("behandl_ende"), transformedIndices.get("behandl_ende"));
        transformedIndices.containsKey("bezugsjahr");
        assertEquals(indices.get("bezugsjahr"), transformedIndices.get("bezugsjahr"));

        List<RowElement> transformedColumns = transformedRow.getColumns();
        assertNotNull(transformedColumns);
        assertEquals(columns.get(0), transformedColumns.get(0));
        assertEquals(columns.get(1), transformedColumns.get(1));
        assertEquals(expectedStartDate, transformedColumns.get(2).getContent());
        assertEquals(expectedEndDate, transformedColumns.get(3).getContent());
        assertEquals(columns.get(4), transformedColumns.get(4));
    }

    private void testStartDateModification(String startQuarter, String expectedStartDate) {
        Row row = createRowWithoutStartDate(startQuarter);
        Map<String, Integer> indices = row.getColumnName2Index();
        List<RowElement> columns = row.getColumns();
        Row transformedRow = new FixMissingDateEntries().transform(row);
        assertNotNull(transformedRow);

        Map<String, Integer> transformedIndices = transformedRow.getColumnName2Index();
        assertNotNull(transformedIndices);
        transformedIndices.containsKey("vertrags_id");
        assertEquals(indices.get("vertrags_id"), transformedIndices.get("vertrags_id"));
        transformedIndices.containsKey("behandl_quartal");
        assertEquals(indices.get("behandl_quartal"), transformedIndices.get("behandl_quartal"));
        transformedIndices.containsKey("behandl_beginn");
        assertEquals(indices.get("behandl_beginn"), transformedIndices.get("behandl_beginn"));
        transformedIndices.containsKey("behandl_ende");
        assertEquals(indices.get("behandl_ende"), transformedIndices.get("behandl_ende"));
        transformedIndices.containsKey("bezugsjahr");
        assertEquals(indices.get("bezugsjahr"), transformedIndices.get("bezugsjahr"));

        List<RowElement> transformedColumns = transformedRow.getColumns();
        assertNotNull(transformedColumns);
        assertEquals(columns.get(0), transformedColumns.get(0));
        assertEquals(columns.get(1), transformedColumns.get(1));
        assertEquals(expectedStartDate, transformedColumns.get(2).getContent());
        assertEquals("20100331", transformedColumns.get(3).getContent());
        assertEquals(columns.get(4), transformedColumns.get(4));
    }

    private void testEndDateModification(String startQuarter, String expectedEndDate) {
        Row row = createRowWithoutEndDate(startQuarter);
        Map<String, Integer> indices = row.getColumnName2Index();
        List<RowElement> columns = row.getColumns();
        Row transformedRow = new FixMissingDateEntries().transform(row);
        assertNotNull(transformedRow);

        Map<String, Integer> transformedIndices = transformedRow.getColumnName2Index();
        assertNotNull(transformedIndices);
        transformedIndices.containsKey("vertrags_id");
        assertEquals(indices.get("vertrags_id"), transformedIndices.get("vertrags_id"));
        transformedIndices.containsKey("behandl_quartal");
        assertEquals(indices.get("behandl_quartal"), transformedIndices.get("behandl_quartal"));
        transformedIndices.containsKey("behandl_beginn");
        assertEquals(indices.get("behandl_beginn"), transformedIndices.get("behandl_beginn"));
        transformedIndices.containsKey("behandl_ende");
        assertEquals(indices.get("behandl_ende"), transformedIndices.get("behandl_ende"));
        transformedIndices.containsKey("bezugsjahr");
        assertEquals(indices.get("bezugsjahr"), transformedIndices.get("bezugsjahr"));

        List<RowElement> transformedColumns = transformedRow.getColumns();
        assertNotNull(transformedColumns);
        assertEquals(columns.get(0), transformedColumns.get(0));
        assertEquals(columns.get(1), transformedColumns.get(1));
        assertEquals("20100101", transformedColumns.get(2).getContent());
        assertEquals(expectedEndDate, transformedColumns.get(3).getContent());
        assertEquals(columns.get(4), transformedColumns.get(4));
    }

    private Row createRowWithoutDates(String startQuarter) {
        List<RowElement> columns = new ArrayList<>(3);
        columns.add(new SimpleRowElement("KV", TeradataColumnType.VARCHAR));
        columns.add(new SimpleRowElement(startQuarter, TeradataColumnType.VARCHAR));
        columns.add(new SimpleRowElement("", TeradataColumnType.VARCHAR));
        columns.add(new SimpleRowElement("", TeradataColumnType.VARCHAR));
        columns.add(new SimpleRowElement("2010", TeradataColumnType.VARCHAR));
        Map<String, Integer> columnIndices = new HashMap<>(4);
        columnIndices.put("vertrags_id", 0);
        columnIndices.put("behandl_quartal", 1);
        columnIndices.put("behandl_beginn", 2);
        columnIndices.put("behandl_ende", 3);
        columnIndices.put("bezugsjahr", 4);
        return new Row("ADB", "Arzt_Fall", columns, columnIndices);
    }

    private Row createRowWithoutStartDate(String startQuarter) {
        List<RowElement> columns = new ArrayList<>(3);
        columns.add(new SimpleRowElement("KV", TeradataColumnType.VARCHAR));
        columns.add(new SimpleRowElement(startQuarter, TeradataColumnType.VARCHAR));
        columns.add(new SimpleRowElement("", TeradataColumnType.VARCHAR));
        columns.add(new SimpleRowElement("20100331", TeradataColumnType.VARCHAR));
        columns.add(new SimpleRowElement("2010", TeradataColumnType.VARCHAR));
        Map<String, Integer> columnIndices = new HashMap<>(4);
        columnIndices.put("vertrags_id", 0);
        columnIndices.put("behandl_quartal", 1);
        columnIndices.put("behandl_beginn", 2);
        columnIndices.put("behandl_ende", 3);
        columnIndices.put("bezugsjahr", 4);
        return new Row("ADB", "Arzt_Fall", columns, columnIndices);
    }

    private Row createRowWithoutEndDate(String startQuarter) {
        List<RowElement> columns = new ArrayList<>(3);
        columns.add(new SimpleRowElement("KV", TeradataColumnType.VARCHAR));
        columns.add(new SimpleRowElement(startQuarter, TeradataColumnType.VARCHAR));
        columns.add(new SimpleRowElement("20100101", TeradataColumnType.VARCHAR));
        columns.add(new SimpleRowElement("", TeradataColumnType.VARCHAR));
        columns.add(new SimpleRowElement("2010", TeradataColumnType.VARCHAR));
        Map<String, Integer> columnIndices = new HashMap<>(4);
        columnIndices.put("vertrags_id", 0);
        columnIndices.put("behandl_quartal", 1);
        columnIndices.put("behandl_beginn", 2);
        columnIndices.put("behandl_ende", 3);
        columnIndices.put("bezugsjahr", 4);
        return new Row("ADB", "Arzt_Fall", columns, columnIndices);
    }

}
