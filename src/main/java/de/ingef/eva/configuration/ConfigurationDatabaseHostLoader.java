package de.ingef.eva.configuration;

import java.util.List;

import de.ingef.eva.configuration.export.SourceConfig;
import de.ingef.eva.configuration.export.ViewConfig;
import de.ingef.eva.database.DatabaseHost;
import de.ingef.eva.database.TextDatabase;
import de.ingef.eva.database.TextSchema;
import de.ingef.eva.database.TextTable;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class ConfigurationDatabaseHostLoader {
	
	public DatabaseHost createDatabaseHost(Configuration config) {
		List<SourceConfig> sources = config.getSources();
		if(sources == null || sources.isEmpty()) {
			log.error("No sources specified.");
			return null;
		}
		TextSchema schema = new TextSchema();
		sources
			.stream()
			.filter(this::isValidSource)
			.forEach(source -> {
				TextDatabase db = new TextDatabase(source.getDb());
				source.getViews()
					.stream()
					.filter(this::isValidView)
					.forEach(view -> {
						db.addTable(new TextTable(view.getName()));
					});
				schema.addDatabase(db);
			});
		
		return schema;
	}
	
	private boolean isValidSource(SourceConfig source) {
		return source.getDb() != null && !source.getDb().isEmpty() && source.getViews() != null && !source.getViews().isEmpty();
	}
	
	private boolean isValidView(ViewConfig view) {
		return view.getName() != null && !view.getName().isEmpty();
	}
}
