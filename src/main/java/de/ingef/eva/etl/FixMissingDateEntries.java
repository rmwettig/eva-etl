package de.ingef.eva.etl;

import de.ingef.eva.data.RowElement;
import de.ingef.eva.data.SimpleRowElement;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Quick fix for https://lyo-pgitl01.spectrumk.ads/eva4/dokumentation/issues/262
 */
public class FixMissingDateEntries extends Transformer {

    private static final String YEAR_COLUMN = "bezugsjahr";
    private static final String CONTRACT_COLUMN = "vertrags_id";
    private static final String QUARTER_COLUMN = "behandl_quartal";
    private static final String START_DATE_COLUMN = "behandl_beginn";
    private static final String END_DATE_COLUMN = "behandl_ende";
    private static final String CONTRACT_ID = "KV";

    public FixMissingDateEntries() {
        super("ADB", "Arzt_Fall");
    }

    @Override
    public Row transform(Row row) {
        if(!canProcessRow(row.getDb(), row.getTable()))
            return row;

        Map<String, Integer> indices = row.getColumnName2Index();
        //check mandatory columns
        if(!indices.containsKey(CONTRACT_COLUMN) ||
                !indices.containsKey(QUARTER_COLUMN) ||
                !indices.containsKey(START_DATE_COLUMN) ||
                !indices.containsKey(END_DATE_COLUMN) ||
                !indices.containsKey(YEAR_COLUMN)
        )
            return row;
        String contractId = row.getColumns().get(indices.get(CONTRACT_COLUMN)).getContent();
        if(!contractId.equalsIgnoreCase(CONTRACT_ID))
            return row;

        List<RowElement> transformedColumns = transformColumns(row.getColumns(), row.getColumnName2Index());
        Map<String, Integer> transformedIndices = new HashMap<>(row.getColumnName2Index());
        return new Row(row.getDb(), row.getTable(), transformedColumns, transformedIndices);
    }

    private List<RowElement> transformColumns(List<RowElement> columns, Map<String, Integer> indices) {
        int yearIndex = indices.get(YEAR_COLUMN);
        String year = columns.get(yearIndex).getContent();
        int quarterIndex = indices.get(QUARTER_COLUMN);
        String quarter = columns.get(quarterIndex).getContent();
        int startDateIndex = indices.get(START_DATE_COLUMN);
        String startDate = columns.get(startDateIndex).getContent();
        int endDateIndex = indices.get(END_DATE_COLUMN);
        String endDate = columns.get(endDateIndex).getContent();

        List<RowElement> transformedColumns = new ArrayList<>(columns);
        if(startDate.isEmpty()) {
            transformedColumns.set(
                    startDateIndex,
                    new SimpleRowElement(year + findStartDate(quarter), columns.get(startDateIndex).getType())
            );
        }
        if(endDate.isEmpty()) {
            transformedColumns.set(
                    endDateIndex,
                    new SimpleRowElement(year + findEndDate(quarter), columns.get(endDateIndex).getType())
            );
        }
        return transformedColumns;
    }

    private String findStartDate(String quarter) {
        switch (quarter) {
            case "1":
                return "0101";
            case "2":
                return "0401";
            case "3":
                return "0701";
            case "4":
                return "1001";
            default:
                return "0101";
        }
    }

    private String findEndDate(String quarter) {
        switch (quarter) {
            case "1":
                return "0331";
            case "2":
                return "0630";
            case "3":
                return "0930";
            case "4":
                return "1231";
            default:
                return "0331";
        }
    }
}
