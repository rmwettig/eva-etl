package de.ingef.eva.etl;

import java.util.List;
import java.util.Map;

import de.ingef.eva.data.RowElement;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public class Row {
	private final String db;
	private final String table;
	private final List<RowElement> columns;
	private final Map<String,Integer> columnName2Index;
}
