package de.ingef.eva.query;

import java.util.Collection;

import de.ingef.eva.configuration.Configuration;
import de.ingef.eva.configuration.SchemaDatabaseHostLoader;
import de.ingef.eva.configuration.SqlJsonInterpreter;
import de.ingef.eva.database.DatabaseHost;
import de.ingef.eva.query.creation.QueryCreator;
import de.ingef.eva.query.creation.SimpleQueryCreator;
import de.ingef.eva.utility.Alias;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;

/**
 * Creates sql query strings
 * @author Martin.Wettig
 *
 */
@RequiredArgsConstructor
@Log4j2
public class JsonQuerySource implements QuerySource {
	private final Configuration configuration;
	
	@Override
	public Collection<Query> createQueries() {
		DatabaseHost schema = new SchemaDatabaseHostLoader().loadFromFile(configuration.getSchemaFile());
		QueryCreator queryCreator = new SimpleQueryCreator(schema);
		queryCreator.setAliasFactory(new Alias(120));
		SqlJsonInterpreter jsonInterpreter = new SqlJsonInterpreter(queryCreator, schema);
		return jsonInterpreter.interpret(configuration.getDatabases());
	}

}
