package de.ingef.eva.query;

import java.util.Collection;

public interface QuerySource {
	Collection<Query> createQueries();
}
