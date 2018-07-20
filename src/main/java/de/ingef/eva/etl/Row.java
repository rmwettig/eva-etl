package de.ingef.eva.etl;

import java.util.List;
import java.util.Map;

import de.ingef.eva.data.RowElement;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter @Setter
@NoArgsConstructor
@AllArgsConstructor
public class Row {
	private String db;
	private String table;
	private List<RowElement> columns;
	private Map<String,Integer> columnName2Index;
}
