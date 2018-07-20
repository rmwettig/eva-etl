package de.ingef.eva.configuration;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import de.ingef.eva.configuration.export.SourceConfig;
import de.ingef.eva.configuration.export.ViewConfig;
import de.ingef.eva.constant.Templates;
import de.ingef.eva.data.TeradataColumnType;
import de.ingef.eva.database.Column;
import de.ingef.eva.database.Database;
import de.ingef.eva.database.DatabaseSchema;
import de.ingef.eva.database.Table;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class SchemaFactory {
	
	public DatabaseSchema createSchema(Configuration config) {
		List<SourceConfig> sources = config.getSources();
		if(sources == null || sources.isEmpty()) {
			log.error("No sources specified.");
			return null;
		}
		
		return fetchSchema(config);
	}
	
	private boolean isValidSource(SourceConfig source) {
		return source.getDb() != null && !source.getDb().isEmpty() && source.getViews() != null && !source.getViews().isEmpty();
	}
	
	private boolean isValidView(ViewConfig view) {
		return view.getName() != null && !view.getName().isEmpty();
	}
	
	private DatabaseSchema fetchSchema(Configuration config) {
		try (Connection connection = DriverManager.getConnection(
				config.getFullConnectionUrl(),
				config.getUser(),
				config.getPassword()
				);
				Statement stm = connection.createStatement();
			) {
			List<Database> databases =
					config.getSources()
					.stream()
					.filter(this::isValidSource)
					.map(source -> {
						Database db = new Database(source.getDb());
						source.getViews()
						.stream()
						.filter(this::isValidView)
						.map(view -> convertToTable(stm, source.getDb(), view.getName()))
						.forEachOrdered(t -> db.addTable(t));
						source.getViews()
						.stream()
						.filter(view -> view.getJoins() != null || view.getJoins().isEmpty())
						.flatMap(view -> view.getJoins().stream())
						.map(join -> convertToTable(stm, source.getDb(), join.getName()))
						.forEachOrdered(t -> db.addTable(t));
						return db;
					})
					.collect(Collectors.toList());
			return mergeDatabases(databases);
		} catch (SQLException e) {
			log.error("Could not open connection to '{}'. ", config.getFullConnectionUrl(), e);
			return new DatabaseSchema();
		}
	}
	
	private DatabaseSchema mergeDatabases(List<Database> databases) {
		DatabaseSchema schema = new DatabaseSchema();
		for(Database db : databases) {
			Optional<Database> existingDb = schema.findDatabaseByName(db.getName());
			if(!existingDb.isPresent()) {
				schema.addDatabase(db);
				continue;
			}
			Database previousDb = existingDb.get();
			db.getAllTables().stream().forEach(table -> {
				Optional<Table> optionalTable = previousDb.findTableByName(table.getName());
				if(!optionalTable.isPresent())
					previousDb.addTable(table);
			});
		}
		return schema;
	}

	/**
	 * fetches columns for the given table from the database
	 * @param stm
	 * @param db
	 * @param tableConfig
	 * @return table without columns on error
	 */
	private Table convertToTable(Statement stm, String db, String tableName) {
		Table t = new Table(tableName);
		try (ResultSet rs = stm.executeQuery(String.format(Templates.QUERY_COLUMNS, db, tableName))){
			while(rs.next()) {
				t.addColumn(new Column(rs.getString(1).trim(), TeradataColumnType.mapCodeToName(rs.getString(2).trim())));
			}
			return t;
		} catch (SQLException e) {
			log.error("Could not fetch columns for {}.{}. {}", db, tableName, e);
			return t;
		}
	}
}
