package de.ingef.eva.query;

import java.util.Collection;
import java.util.stream.Collectors;

import de.ingef.eva.configuration.Configuration;
import de.ingef.eva.configuration.SchemaDatabaseHostLoader;
import de.ingef.eva.database.DatabaseHost;
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
		DatabaseHost schema = new SchemaDatabaseHostLoader().loadFromFile(configuration.getSchemaFile());
		QueryCreator queryCreator = new SimpleQueryCreator(schema);
		return configuration.getExport().getSources()
				.stream()
				.flatMap(source -> source.traverse(queryCreator).stream())
				.collect(Collectors.toList());
	}

}
