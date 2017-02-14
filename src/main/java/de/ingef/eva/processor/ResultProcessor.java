package de.ingef.eva.processor;

import java.util.Collection;

public interface ResultProcessor {
	Collection<String[]> ProcessResults(Collection<String[]> results);
}
