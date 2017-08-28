package de.ingef.eva.query;

import de.ingef.eva.data.DataTable;
import de.ingef.eva.error.QueryExecutionException;

public interface QueryExecutor<T> {
	DataTable execute(T query) throws QueryExecutionException;
}
