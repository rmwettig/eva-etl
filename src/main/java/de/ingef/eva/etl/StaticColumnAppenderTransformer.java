package de.ingef.eva.etl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import de.ingef.eva.configuration.append.AppendOrder;
import de.ingef.eva.data.RowElement;
import de.ingef.eva.data.SimpleRowElement;
import de.ingef.eva.data.TeradataColumnType;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class StaticColumnAppenderTransformer extends Transformer {

	private final String targetDb;
	private final String targetTable;
	private final String valueName;
	private final String value;
	private final AppendOrder order;
		
	@Override
	public Row transform(Row row) {
		if(!row.getDb().contains(targetDb) || !row.getTable().contains(targetTable))
			return row;
		
		List<RowElement> columns = row.getColumns();
		List<RowElement> transformed = new ArrayList<>(columns.size() + 1);
		Map<String,Integer> indices = row.getColumnName2Index();
		Map<String,Integer> transformedIndices = new HashMap<>(indices.size() + 1);
		if(order == AppendOrder.FIRST) {
			transformed.add(new SimpleRowElement(value, TeradataColumnType.CHARACTER));
			transformed.addAll(columns);
			transformedIndices.put(valueName, 0);
			indices.forEach((column,index) -> transformedIndices.put(column, index + 1)); 
		} else {
			transformed.addAll(columns);
			transformed.add(new SimpleRowElement(value, TeradataColumnType.CHARACTER));
			transformedIndices.putAll(indices);
			//size equals the index of the newly added column
			transformedIndices.put(valueName, indices.size());
		}
		
		return new Row(row.getDb(), row.getTable(), transformed, transformedIndices);
	}
}
