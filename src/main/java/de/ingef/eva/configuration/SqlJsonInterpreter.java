package de.ingef.eva.configuration;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import de.ingef.eva.configuration.export.ExportConfig;
import de.ingef.eva.configuration.export.JoinConfig;
import de.ingef.eva.configuration.export.JoinType;
import de.ingef.eva.configuration.export.SourceConfig;
import de.ingef.eva.configuration.export.ViewConfig;
import de.ingef.eva.configuration.export.WhereConfig;
import de.ingef.eva.configuration.export.WhereOperator;
import de.ingef.eva.configuration.export.WhereType;
import de.ingef.eva.database.Column;
import de.ingef.eva.database.Database;
import de.ingef.eva.database.DatabaseHost;
import de.ingef.eva.database.Table;
import de.ingef.eva.query.Query;
import de.ingef.eva.query.creation.QueryCreator;
import de.ingef.eva.utility.Helper;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class SqlJsonInterpreter {
	private DatabaseHost _schema;
	private QueryCreator _queryCreator;
	private Collection<Query> _jobs;

	public SqlJsonInterpreter(QueryCreator queryCreator, DatabaseHost schema) {
		_queryCreator = queryCreator;
		_schema = schema;
		_jobs = new ArrayList<Query>(20);
	}

	/**
	 * Evaluates the object associated to the 'databases' field
	 * 
	 * @param exportConfig
	 *            object with settings for query creation
	 * @return an empty collection if no query could be built
	 */
	public Collection<Query> interpret(ExportConfig exportConfig) {
		if(exportConfig == null) {
			log.error("No dump configuration present!");
			return _jobs;
		}
		
		int[] years = calculateYearSlices(exportConfig);
		
		List<SourceConfig> sources = exportConfig.getSources();
		if (sources.isEmpty()) {
			log.error("Did not found 'sources' key or is not an array.");
			return _jobs;
		}

		for(SourceConfig source : sources) {
			if(source.getDb() == null || source.getDb().isEmpty())
				continue;
			_queryCreator.setDatabase(source.getDb());
			findViews(source, _schema.findDatabaseByName(source.getDb()), years, source.getDatasetName());
		}

		return _jobs;
	}
	
	private int[] calculateYearSlices(ExportConfig exportConfig) {
		int numberOfPreviousYears = exportConfig.getNumberOfPreviousYears();
		if(numberOfPreviousYears > 0) {
			//take only the recent year
			int endYear = Calendar.getInstance().get(Calendar.YEAR);
			int startYear = endYear - numberOfPreviousYears;
			return Helper.extractYears(startYear, endYear);
		}
		
		int startYear = exportConfig.getStartYear();
		if (startYear == 0) {
			log.error("Did not found 'startYear' key.");
			return new int[1];
		}
		int endYear = exportConfig.getEndYear() == 0 ? Calendar.getInstance().get(Calendar.YEAR) : exportConfig.getEndYear();
				
		return Helper.extractYears(startYear, endYear);
	}

	/**
	 * Processes tables in the 'views' entry
	 * 
	 * @param source
	 * @param schemaDatabase
	 * @param years
	 * @param dataset TODO
	 * @return false if 'views' entry is missing
	 */
	private boolean findViews(SourceConfig source, Database schemaDatabase, int[] years, String dataset) {
		List<ViewConfig> views = source.getViews();
		for (ViewConfig view : views) {
			String tableName = view.getName();
			// do not return here as one view might fail but others work
			Table t = schemaDatabase.findTableByName(tableName);
			if (t != null) {
				if (t.findColumnByName("Bezugsjahr") != null)
					processViewByYear(source, view, tableName, "Bezugsjahr", schemaDatabase, years, dataset);
				else
					processView(source, view, tableName, schemaDatabase, dataset);
			} else {
				log.warn("Did not found an table entry for '{}' in database '{}'", tableName, schemaDatabase.getName());
			}

		}

		return true;
	}

	/**
	 * Iterates over the years array and creates a query per year
	 * 
	 * @param source
	 * @param view
	 * @param tableName
	 * @param yearColumn
	 * @param schemaDatabase
	 * @param years
	 * @param dataset TODO
	 * @return false if view could not be processed
	 */
	private boolean processViewByYear(SourceConfig source, ViewConfig view, String tableName, String yearColumn, Database schemaDatabase, int[] years, String dataset) {
		for (int year : years) {
			_queryCreator.startOrGroup();
			_queryCreator.addWhere(tableName, yearColumn, Integer.toString(year), WhereOperator.EQUAL.getSymbol(), WhereType.NUMERIC.name());
			_queryCreator.endOrGroup(tableName);
			if (!processView(source, view, tableName, schemaDatabase, Integer.toString(year), dataset))
				return false;
		}
		return true;
	}

	private boolean processView(SourceConfig source, ViewConfig view, String tableName, Database schemaDatabase, String partLabel, String dataset) {
		_queryCreator.addTable(tableName);
		
		if(view.isLatest()) {
			Query q = evaluateLatestField(tableName, schemaDatabase.getName(), view, schemaDatabase.findTableByName(tableName).getAllColumns());
			if(q != null)
				_jobs.add(q);
			
			return true;
		}
		if (!evaluateColumnEntry(view, tableName, schemaDatabase.findTableByName(tableName).getAllColumns())) {
			log.error("Did not found any columns.\nCheck 'column' field or database schema file for table '{}'.", tableName);
			return false;
		}
		evaluateWhereEntry(view.getWhere(), tableName);
		// extract global conditions from source node
		evaluateWhereEntry(source.getWhere(), tableName);
		evaluateJoinEntry(view, tableName, schemaDatabase);
		Query query = _queryCreator.buildQuery();
		String qName = schemaDatabase.getName() + "_" + tableName;
		qName += (partLabel.isEmpty()) ? partLabel : "." + partLabel;
		query.setName(qName);
		query.setDbName(schemaDatabase.getName());
		query.setSliceName(partLabel);
		query.setTableName(tableName);
		query.setDatasetName(dataset);

		_jobs.add(query);

		return true;
	}

	/**
	 * Creates a query for a single unsliced view
	 * 
	 * @param source
	 * @param view
	 * @param tableName
	 * @param schemaDatabase
	 * @return null if no columns are selected
	 */
	private boolean processView(SourceConfig source, ViewConfig view, String tableName, Database schemaDatabase, String dataset) {
		return processView(source, view, tableName, schemaDatabase, "", dataset);
	}

	/**
	 * Adds specified columns to the result set or takes all available from the
	 * schema
	 * 
	 * @param selectParameters
	 *            parent node that holds the 'columns' entry
	 * @param tableName
	 *            name of the table for which columns are added
	 * @param schemaColumns
	 *            available columns
	 * @return false if neither a 'columns' entry is given nor columns exist in
	 *         the schema
	 */
	private boolean evaluateColumnEntry(ViewConfig selectParameters, String tableName, Collection<Column> schemaColumns) {
		List<String> columns = selectParameters.getColumns();
		Set<String> excludedColumns = convertToLookup(selectParameters.getExcludeColumns());
		if (columns == null || columns.isEmpty()) {
			if(schemaColumns == null)
				return false;
			List<Column> filteredColumns = removeExcludedColumns(schemaColumns, excludedColumns);
			for (Column c : filteredColumns)
				_queryCreator.addColumn(tableName, c.getName());
		} else {
			List<String> filteredColumns = removeExcludedColumns(columns, excludedColumns);
			for (String column : filteredColumns) {
				_queryCreator.addColumn(tableName, column);
			}
		}
		return true;
	}
	
	private Set<String> convertToLookup(List<String> excludeColumns) {
		if(excludeColumns == null || excludeColumns.isEmpty())
			return new HashSet<String>(1);
		return excludeColumns
				.stream()
				.collect(Collectors.toSet());
	}
	
	private List<String> removeExcludedColumns(List<String> columns, Set<String> excludeColumns) {
		return columns
				.stream()
				.filter(column -> !excludeColumns.contains(column))
				.collect(Collectors.toList());
	}

	private List<Column> removeExcludedColumns(Collection<Column> schemaColumns, Set<String> excludeColumns) {
		return schemaColumns
				.stream()
				.filter(column -> !excludeColumns.contains(column.getName()))
				.collect(Collectors.toList());
	}

	private void evaluateWhereEntry(List<WhereConfig> wheres, String tableName) {
		// where entry is optional so skip everything if absent
		if (wheres == null || wheres.isEmpty())
			return;
		
		createWhereMap(wheres)
			.forEach((column, constraints) -> {
				_queryCreator.startOrGroup();
				constraints
					.stream()
					.forEach(where ->
						_queryCreator.addWhere(tableName, column, where.getValue(), where.getOperator().getSymbol(), where.getType().name()));
				_queryCreator.endOrGroup(tableName);
			});
	}

	private Map<String, List<WhereConfig>> createWhereMap(List<WhereConfig> wheres) {
		return wheres
				.stream()
				.flatMap(this::evaluateWhereConfig)
				.collect(Collectors.groupingBy(WhereConfig::getColumn));
	}
	
	private Stream<WhereConfig> evaluateWhereConfig(WhereConfig config) {
		if(config.getSource() == WhereSource.FILE)
			return processFileCondition(config);
		//if it is not a file condition take the current object
		return Stream.<WhereConfig>builder().add(config).build();
	}
	
	private Stream<WhereConfig> processFileCondition(WhereConfig config) {
		Set<String> ids = Helper.createUniqueLookupFromFile(config.getValue());
		return ids
				.stream()
				.map(value -> new WhereConfig(config.getColumn(), value, config.getOperator(), config.getType(), WhereSource.PLAIN))
				.collect(Collectors.toList())
				.stream();
	}

	private void evaluateJoinEntry(ViewConfig selectParameters, String leftTable, Database schemaDatabase) {
		List<JoinConfig> joinConfigs = selectParameters.getJoin();
		
		// optional node
		if (joinConfigs == null || joinConfigs.isEmpty())
			return;

		joinConfigs
			.stream()
			.forEach(join -> {
				//add selected column from join
				addJoinedColumns(schemaDatabase, join);
				//add columns used for on condition
				addOnColumns(leftTable, join);
				//take additional conditions for the join into account
				evaluateWhereEntry(join.getWhere(), join.getTable());
			});	
		}

	private void addOnColumns(String leftTable, JoinConfig join) {
		join.getOn()
			.stream()
			.forEach(column -> _queryCreator.addJoin(leftTable, join.getTable(), column, join.getType().getName()));
	}

	private void addJoinedColumns(Database schemaDatabase, JoinConfig join) {
		String table = join.getTable();
		join.getColumn()
			.stream()
			.forEach(column -> {
				if(!column.equalsIgnoreCase("*")) {
					_queryCreator.addColumn(table, column);
				} else {
					List<Column> schemaColumns = schemaDatabase.findTableByName(table).getAllColumns();
					for (Column schemaColumn : schemaColumns) {
						_queryCreator.addColumn(table, schemaColumn.getName());
					}
				}
			});
	}
		
	private Query evaluateLatestField(String currentTable, String database, ViewConfig latestView, Collection<Column> schemaColumns) {
		String id = latestView.getIdColumn();
		String timestamp = latestView.getTimestamp();
		if(id == null || id.isEmpty() || timestamp == null || timestamp.isEmpty())
			return null;
		
		String rightTable = "(select %s, max(%s) as %s from %s.%s group by %s)";
		rightTable = String.format(rightTable, id, timestamp, timestamp, database, currentTable, id);
		
		for(Column c : schemaColumns)
			_queryCreator.addColumn(currentTable, c.getName());
		_queryCreator.addJoin(currentTable, rightTable, id, JoinType.INNER.getName());
		_queryCreator.startOrGroup();
		_queryCreator.addWhere(rightTable, timestamp, "", WhereOperator.EQUAL.getSymbol(), WhereType.COLUMN.name());
		_queryCreator.endOrGroup(currentTable);
		
		Query q = _queryCreator.buildQuery();
		q.setName(database+"_"+currentTable);
		
		return q;
	}
}
