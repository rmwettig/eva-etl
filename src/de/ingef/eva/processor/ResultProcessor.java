package de.ingef.eva.processor;

import java.sql.ResultSet;
import java.util.Collection;

public interface ResultProcessor {
	Collection<String> ProcessResults(ResultSet results);
}
