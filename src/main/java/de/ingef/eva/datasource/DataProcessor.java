package de.ingef.eva.datasource;

import de.ingef.eva.data.DataTable;

public interface DataProcessor {
	public DataTable process(DataTable... dataTables);
}
