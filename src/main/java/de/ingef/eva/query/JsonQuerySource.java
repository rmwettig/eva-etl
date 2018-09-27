package de.ingef.eva.query;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import de.ingef.eva.configuration.Configuration;
import de.ingef.eva.configuration.SchemaFactory;
import de.ingef.eva.database.DatabaseSchema;
import de.ingef.eva.query.creation.QueryCreator;
import de.ingef.eva.query.creation.SimpleQueryCreator;
import lombok.RequiredArgsConstructor;

/**
 * Creates sql query strings
 * @author Martin.Wettig
 *
 */
@RequiredArgsConstructor
public class JsonQuerySource implements QuerySource {
	private final Configuration configuration;
	
	@Override
	public Collection<Query> createQueries() {
		DatabaseSchema schema = new SchemaFactory().createSchema(configuration);
		QueryCreator queryCreator = new SimpleQueryCreator(schema);
		List<Query> queries = configuration.getSources()
				.stream()
				.flatMap(source -> source.traverse(queryCreator).stream())
				.collect(Collectors.toList());
		//avoid runs of queries accessing the same table
		Collections.shuffle(queries);
		return queries;
	}
}
