package de.ingef.eva.etl.transformers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import de.ingef.eva.data.RowElement;
import de.ingef.eva.data.SimpleRowElement;
import de.ingef.eva.data.TeradataColumnType;
import de.ingef.eva.etl.Row;

public class PharmacyTypeTransformer extends Transformer {

	private static final String EMPTY_DEFAULT = "";
	private static final String MISCELLANEOUS_TYPE = "00";
	private static final String PHARMACY_TYPE = "30";
	private static final String HOSPITAL_TYPE = "26";
	private static final String PHARMACY_IK_COLUMN = "apothekenik";
	private static final String PHARMACY_TYPE_COLUMN = "apo_typ";
	
	public PharmacyTypeTransformer(String db, String table) {
		super(db, table);
	}
	
	@Override
	public Row transform(Row row) {
		if(!canProcessRow(row.getDb(), row.getTable()))
			return row;
		Map<String, Integer> columnIndices = row.getColumnName2Index();
		if(!columnIndices.containsKey(PHARMACY_IK_COLUMN))
			return row;
		String pharmacyType = mapToType(row.getColumns().get(columnIndices.get(PHARMACY_IK_COLUMN)).getContent());
		
		return createRow(row, pharmacyType);
	}
	
	private String extractPrefix(String ik) {
		return ik.substring(0, 2);
	}
	
	/**
	 * map ik prefix to hospital (26), pharmacy (30) or miscellaneous (00)
	 * @param ik
	 * @return
	 */
	private String mapToType(String ik) {
		if(ik == null || ik.isEmpty())
			return EMPTY_DEFAULT;
		String prefix = extractPrefix(ik);
		switch (prefix) {
			case HOSPITAL_TYPE:
			case PHARMACY_TYPE:
				return prefix;
			default:
				return MISCELLANEOUS_TYPE;
		}
	}
	
	private Row createRow(Row row, String pharmacyType) {
		List<RowElement> newColumns = new ArrayList<>(row.getColumns());
		newColumns.add(new SimpleRowElement(pharmacyType, TeradataColumnType.CHARACTER));
		Map<String, Integer> newIndices = new HashMap<>(row.getColumnName2Index());
		newIndices.put(PHARMACY_TYPE_COLUMN, newColumns.size() - 1);
		
		return new Row(row.getDb(), row.getTable(), newColumns, newIndices);
	}
}
