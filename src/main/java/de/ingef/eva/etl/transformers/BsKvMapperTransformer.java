package de.ingef.eva.etl.transformers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import de.ingef.eva.data.RowElement;
import de.ingef.eva.data.SimpleRowElement;
import de.ingef.eva.data.TeradataColumnType;
import de.ingef.eva.etl.Row;

public class BsKvMapperTransformer extends Transformer {

	public BsKvMapperTransformer(String db, String table) {
		super(db, table);
	}

	public BsKvMapperTransformer() {
		super("", "");
	}
	
	@Override
	public Row transform(Row row) {
		//only process evo tables
		if(!row.getTable().toLowerCase().endsWith("evo"))
			return row;
		Map<String, Integer> columnName2Index = row.getColumnName2Index();
		//if bs_nr column does not exist
		if(!columnName2Index.containsKey("bs_nr"))
			return appendEmptyKv(row);
				
		String bsNo = row.getColumns().get(columnName2Index.get("bs_nr")).getContent();
		if(bsNo == null || bsNo.isEmpty() ||  bsNo.length() < 2)
			return appendEmptyKv(row);
		
		String majorKv = mapSubKvToMajorKv(extractSubKvFromBsNo(bsNo));
		return createTransformedRow(row, majorKv);
	}

	private int extractSubKvFromBsNo(String bsNo) {
		return Integer.parseInt(bsNo.substring(0, 2));
	}
	
	private Row appendEmptyKv(Row original) {
		return createTransformedRow(original, "");
	}
	
	private Row createTransformedRow(Row original, String kv) {
		List<RowElement> columns = createNewColumns(original.getColumns(), kv);
		Map<String, Integer> columnName2Index = createNewIndices(original.getColumnName2Index());
		return new Row(original.getDb(), original.getTable(), columns, columnName2Index);
	}
	
	private List<RowElement> createNewColumns(List<RowElement> original, String value) {
		List<RowElement> transformed = new ArrayList<RowElement>(original.size() + 1);
		transformed.addAll(original);
		transformed.add(new SimpleRowElement(value, TeradataColumnType.CHARACTER));
		return transformed;
	}
	
	private Map<String, Integer> createNewIndices(Map<String, Integer> indicies) {
		Map<String, Integer> transformed = new HashMap<>(indicies);
		Optional<Integer> maxIndex = transformed.values().stream().max(Integer::compare);
		transformed.put("kv", maxIndex.get() + 1);
		return transformed;
	}
	
	private String mapSubKvToMajorKv(int subKv) {
		switch(subKv) {
			case 1:
				return "01";
			case 2:
				return "02";
			case 3:
				return "03";
			case 6:
			case 7:
			case 8:
			case 9:
			case 10:
			case 11:
			case 12:
			case 13:
			case 14:
			case 15:
			case 16:
			case 17:
				return "17";
			case 18:
			case 19:
			case 20:
				return "20";
			case 21:
			case 24:
			case 25:
			case 27:
			case 28:
			case 31:
			case 37:
			case 38:
				return "38";
			case 39:
			case 40:
			case 41:
			case 42:
			case 43:
			case 44:
			case 45:
			case 46:
				return "46";
			case 47:
			case 48:
			case 49:
			case 50:
			case 51:
				return "51";				
			case 52:
			case 53:
			case 54:
			case 55:
			case 56:
			case 57:
			case 58:
			case 59:
			case 60:
			case 61:
			case 62:
				return "52";
			case 63:
			case 64:
			case 65:
			case 66:
			case 67:
			case 68:
			case 69:
			case 70:
			case 71:
				return "71";
			case 72:
				return "72";
			case 73:
				return "73";
			case 78:
				return "78";
			case 79:
			case 80:
			case 81:
			case 83:
				return "83";
			case 85:
			case 86:
			case 87:
			case 88:
				return "88";
			case 89:
			case 90:
			case 91:
			case 93:
				return "93";
			case 94:
			case 95:
			case 96:
			case 98:
				return "98";
			default:
				return "";
		}
	}
	
}
