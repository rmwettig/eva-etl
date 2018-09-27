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

import static org.junit.Assert.*;

public class DynamicColumnAppenderTest {

    @Test
    public void appendValuesIfKeysMatch() {
        List<String> keyNames = new ArrayList<>(2);
        keyNames.add("vertrags_id");
        keyNames.add("efn_id");
        List<RowElement> header = new ArrayList<>(2);
        header.add(new SimpleRowElement("value1", TeradataColumnType.VARCHAR));
        header.add(new SimpleRowElement("value2", TeradataColumnType.VARCHAR));
        Map<String, List<RowElement>> key2Columns = new HashMap<>(1);
        List<RowElement> data = new ArrayList<>(2);
        data.add(new SimpleRowElement("1337", TeradataColumnType.VARCHAR));
        data.add(new SimpleRowElement("1", TeradataColumnType.VARCHAR));
        key2Columns.put("1|123", data);
        DynamicColumnAppender transformer = new DynamicColumnAppender("db", "table", keyNames, header, key2Columns);

        List<RowElement> columns = new ArrayList<>(2);
        columns.add(new SimpleRowElement("1", TeradataColumnType.VARCHAR));
        columns.add(new SimpleRowElement("123", TeradataColumnType.VARCHAR));
        Map<String, Integer> index = new HashMap<>();
        index.put("vertrags_id", 0);
        index.put("efn_id", 1);
        Row row = new Row("db", "table", columns, index);
        Row transformedRow = transformer.transform(row);
        assertNotNull(transformedRow);
        assertEquals("db", transformedRow.getDb());
        assertEquals("table", transformedRow.getTable());
        List<RowElement> transformedColumns = transformedRow.getColumns();
        assertNotNull(transformedColumns);
        assertEquals(4, transformedColumns.size());
        assertEquals("1", transformedColumns.get(0).getContent());
        assertEquals("123", transformedColumns.get(1).getContent());
        assertEquals("1337", transformedColumns.get(2).getContent());
        assertEquals("1", transformedColumns.get(3).getContent());

        Map<String, Integer> transformedIndices = transformedRow.getColumnName2Index();
        assertNotNull(transformedColumns);
        assertTrue(transformedIndices.containsKey("vertrags_id"));
        assertEquals(0, transformedIndices.get("vertrags_id").intValue());
        assertTrue(transformedIndices.containsKey("efn_id"));
        assertEquals(1, transformedIndices.get("efn_id").intValue());
        assertTrue(transformedIndices.containsKey("value1"));
        assertEquals(2, transformedIndices.get("value1").intValue());
        assertTrue(transformedIndices.containsKey("value2"));
        assertEquals(3, transformedIndices.get("value2").intValue());
    }

    @Test
    public void appendEmptyColumnsIfKeysMismatch() {
        List<String> keyNames = new ArrayList<>(2);
        keyNames.add("vertrags_id");
        keyNames.add("efn_id");
        List<RowElement> header = new ArrayList<>(2);
        header.add(new SimpleRowElement("value1", TeradataColumnType.VARCHAR));
        header.add(new SimpleRowElement("value2", TeradataColumnType.VARCHAR));
        Map<String, List<RowElement>> key2Columns = new HashMap<>(1);
        List<RowElement> data = new ArrayList<>(2);
        data.add(new SimpleRowElement("1337", TeradataColumnType.VARCHAR));
        data.add(new SimpleRowElement("1", TeradataColumnType.VARCHAR));
        key2Columns.put("1|123", data);
        DynamicColumnAppender transformer = new DynamicColumnAppender("db", "table", keyNames, header, key2Columns);

        List<RowElement> columns = new ArrayList<>(2);
        columns.add(new SimpleRowElement("1", TeradataColumnType.VARCHAR));
        columns.add(new SimpleRowElement("125", TeradataColumnType.VARCHAR));
        Map<String, Integer> index = new HashMap<>();
        index.put("vertrags_id", 0);
        index.put("efn_id", 1);
        Row row = new Row("db", "table", columns, index);
        Row transformedRow = transformer.transform(row);
        assertNotNull(transformedRow);
        assertEquals("db", transformedRow.getDb());
        assertEquals("table", transformedRow.getTable());
        List<RowElement> transformedColumns = transformedRow.getColumns();
        assertNotNull(transformedColumns);
        assertEquals(4, transformedColumns.size());
        assertEquals("1", transformedColumns.get(0).getContent());
        assertEquals("125", transformedColumns.get(1).getContent());
        assertTrue(transformedColumns.get(2).getContent().isEmpty());
        assertTrue(transformedColumns.get(3).getContent().isEmpty());

        Map<String, Integer> transformedIndices = transformedRow.getColumnName2Index();
        assertNotNull(transformedColumns);
        assertTrue(transformedIndices.containsKey("vertrags_id"));
        assertEquals(0, transformedIndices.get("vertrags_id").intValue());
        assertTrue(transformedIndices.containsKey("efn_id"));
        assertEquals(1, transformedIndices.get("efn_id").intValue());
        assertTrue(transformedIndices.containsKey("value1"));
        assertEquals(2, transformedIndices.get("value1").intValue());
        assertTrue(transformedIndices.containsKey("value2"));
        assertEquals(3, transformedIndices.get("value2").intValue());
    }
}
