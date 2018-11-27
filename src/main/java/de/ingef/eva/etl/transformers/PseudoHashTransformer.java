package de.ingef.eva.etl.transformers;

import de.ingef.eva.data.RowElement;
import de.ingef.eva.data.SimpleRowElement;
import de.ingef.eva.data.TeradataColumnType;
import de.ingef.eva.etl.Row;
import org.apache.commons.codec.digest.DigestUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Workaround for adding a hash column to vers_stamm.
 * It adds a SHA-256 of the pid to the rows
 */
public class PseudoHashTransformer extends Transformer {

    private static final String PID_HASH_COLUMN_NAME = "pid_hash";

    public PseudoHashTransformer() {
        super("", "Vers_Stamm");
    }

    @Override
    public Row transform(Row row) {
        if(!canProcessRow(row.getDb(), row.getTable()))
            return row;

        Map<String, Integer> transformedIndices = transformIndices(row.getColumnName2Index());
        int pidIndex = row.getColumnName2Index().get("pid");
        String hash = DigestUtils.sha256Hex(row.getColumns().get(pidIndex).getContent());
        List<RowElement> transformedColumns = transformColumns(row.getColumns(), hash);
        return new Row(row.getDb(), row.getTable(), transformedColumns, transformedIndices);
    }

    private List<RowElement> transformColumns(List<RowElement> columns, String hash) {
        List<RowElement> transformedColumns = new ArrayList<>(columns.size() + 1);
        transformedColumns.addAll(columns);
        transformedColumns.add(new SimpleRowElement(hash, TeradataColumnType.VARCHAR));
        return transformedColumns;
    }

    private Map<String, Integer> transformIndices(Map<String, Integer> columnName2Index) {
        Map<String, Integer> transformedIndices = new HashMap<>(columnName2Index.size() + 1);
        transformedIndices.putAll(columnName2Index);
        transformedIndices.put(PID_HASH_COLUMN_NAME, columnName2Index.size());
        return transformedIndices;
    }
}
