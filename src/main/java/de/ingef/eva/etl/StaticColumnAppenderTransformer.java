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
import lombok.extern.log4j.Log4j2;

@Log4j2
@RequiredArgsConstructor
public class StaticColumnAppenderTransformer extends Transformer {

	private final String targetDb;
	private final String targetTable;
	private final String valueName;
	private final String value;
	private final AppendOrder order;
		
	@Override
	public Row transform(Row row) {
		if((targetDb != null && !row.getDb().contains(targetDb)) || (targetTable != null && !row.getTable().contains(targetTable)))
			return row;
		
		List<RowElement> columns = row.getColumns();
		Map<String,Integer> indices = row.getColumnName2Index();
		if(hasColumnAlready(indices)) {
			log.warn("Skip row from '{}.{}' as column '{}' exists already.", row.getDb(), row.getTable(), valueName);
			return row;
		}
		List<RowElement> transformed = new ArrayList<>(columns.size() + 1);
		
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

	private boolean hasColumnAlready(Map<String,Integer> columns) {
		return columns.containsKey(valueName);
	}
}
