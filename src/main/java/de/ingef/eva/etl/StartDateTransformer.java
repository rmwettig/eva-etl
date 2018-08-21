package de.ingef.eva.etl;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import de.ingef.eva.data.RowElement;
import de.ingef.eva.data.SimpleRowElement;
import de.ingef.eva.data.TeradataColumnType;

public class StartDateTransformer extends Transformer {

	private String endDateColumn;
	private String dayColumn;
	private String outputColumn;
	
	public StartDateTransformer(String db, String table, String endDateColumn, String dayColumn, String outputColumn) {
		super(db, table);
		this.endDateColumn = endDateColumn;
		this.dayColumn = dayColumn;
		this.outputColumn = outputColumn;
	}
	
	@Override
	public Row transform(Row row) {
		if(!canProcessRow(row.getDb(), row.getTable()))
			return row;
		Optional<Map.Entry<String, Integer>> endDateColumnIndex =
				row.getColumnName2Index().entrySet().stream().filter(entry -> entry.getKey().equalsIgnoreCase(endDateColumn)).findFirst();
		Optional<Map.Entry<String, Integer>> dayColumnIndex =
				row.getColumnName2Index().entrySet().stream().filter(entry -> entry.getKey().equalsIgnoreCase(dayColumn)).findFirst();
		if(!endDateColumnIndex.isPresent() || !dayColumnIndex.isPresent())
			return createEmptyStartDateRow(row);

		LocalDate endDate = LocalDate.parse(row.getColumns().get(endDateColumnIndex.get().getValue()).getContent());
		int kgDays = parseDayCount(row, dayColumnIndex);
		LocalDate startDate = kgDays > 0
			? endDate.minusDays(kgDays).plusDays(1)
			: endDate;

		return new Row(row.getDb(), row.getTable(), transformColumns(row.getColumns(), startDate.toString()), transformIndices(row.getColumnName2Index()));
	}

	/**
	 * tries to convert a string as int
	 * @param row
	 * @param dayColumnIndex
	 * @return parsed number or zero if string is not a number
	 */
	private int parseDayCount(Row row, Optional<Map.Entry<String, Integer>> dayColumnIndex) {
		try {
			return Integer.parseInt(row.getColumns().get(dayColumnIndex.get().getValue()).getContent());
		} catch (NumberFormatException e) {
			return 0;
		}
	}

	private Row createEmptyStartDateRow(Row row) {
		return new Row(row.getDb(), row.getTable(), transformColumns(row.getColumns(), ""), transformIndices(row.getColumnName2Index()));
	}
	
	private Map<String, Integer> transformIndices(Map<String, Integer> oldIndices) {
		Map<String, Integer> transformed = new HashMap<>(oldIndices.size());
		transformed.putAll(oldIndices);
		transformed.put(outputColumn, oldIndices.size());
		return transformed;
	}
	
	private List<RowElement> transformColumns(List<RowElement> columns, String value) {
		List<RowElement> transformed = new ArrayList<>(columns.size() + 1);
		transformed.addAll(columns);
		transformed.add(new SimpleRowElement(value, TeradataColumnType.CHARACTER));
		return transformed;
	}
}
