package de.ingef.eva.processor;

import de.ingef.eva.data.DataTable;

public interface Reducer {
	DataTable reduce(DataTable...tables);
}
