package de.ingef.eva.configuration;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Iterator;
import org.apache.logging.log4j.Logger;
import com.fasterxml.jackson.databind.JsonNode;

import de.ingef.eva.database.Column;
import de.ingef.eva.database.Database;
import de.ingef.eva.database.DatabaseHost;
import de.ingef.eva.database.Table;
import de.ingef.eva.query.Query;
import de.ingef.eva.query.QueryCreator;
import de.ingef.eva.utility.Helper;

public class SqlJsonInterpreter implements JsonInterpreter {

	private Logger _logger;
	private DatabaseHost _schema;
	private QueryCreator _queryCreator;
	private Collection<Query> _jobs;

	public SqlJsonInterpreter(QueryCreator queryCreator, DatabaseHost schema, Logger logger) {
		_queryCreator = queryCreator;
		_schema = schema;
		_logger = logger;
		_jobs = new ArrayList<Query>(20);
	}

	/**
	 * Evaluates the object associated to the 'databases' field
	 * 
	 * @param node
	 *            Json object representing all queried databases
	 * @return an empty collection if no query could be built
	 */
	@Override
	public Collection<Query> interpret(JsonNode node) {

		if (node.isMissingNode()) {
			if (_logger != null)
				_logger.error("'databases' node is empty.");
			return _jobs;
		}
		
		int[] years = calculateYearSlices(node);

		JsonNode sourcesNode = node.path("sources");
		if (sourcesNode.isMissingNode() || !sourcesNode.isArray()) {
			if (_logger != null)
				_logger.error("Did not found 'sources' key or is not an array.");
			return _jobs;
		}

		for (JsonNode source : sourcesNode) {
			String dbName = findDatabaseName(source);
			if (dbName.isEmpty())
				continue;
			_queryCreator.setDatabase(dbName);
			if (!findViews(source, _schema.findDatabaseByName(dbName), years))
				continue;
		}
		return _jobs;
	}

	private int[] calculateYearSlices(JsonNode node) {
		int[] years = null;
		JsonNode previousYears = node.path("numberOfPreviousYears");
		
		if(!previousYears.isMissingNode()) {
			//take only the recent year
			int endYear = Calendar.getInstance().get(Calendar.YEAR);
			int startYear = endYear - previousYears.asInt();
			years = Helper.extractYears(startYear, endYear);
		} else {
			int startYear = extractStartYear(node);
			
			if (startYear == -1) {
				if (_logger != null)
					_logger.error("Did not found 'startYear' key.");
				return years;
			}
			int endYear = extractEndYear(node);
			years = Helper.extractYears(startYear, endYear);
		}
		return years;
	}

	private int extractEndYear(JsonNode dbs) {
		JsonNode node;
		int defaultEndYear = Calendar.getInstance().get(Calendar.YEAR);
		node = dbs.path("endYear");
		if (node.isMissingNode())
			if (_logger != null)
				_logger.warn("Did not found '{}' key.\nUsing default: '{}'", "endYear", defaultEndYear);
		int endYear = node.asInt(defaultEndYear);
		return endYear;
	}

	private int extractStartYear(JsonNode dbs) {
		JsonNode startYearNode = dbs.path("startYear");
		if (startYearNode.isMissingNode())
			return -1;
		int startYear = startYearNode.asInt();
		return startYear;
	}

	/**
	 * Finds the name of the database source
	 * 
	 * @param source
	 *            database source entry
	 * @return empty string if no name is specified
	 */
	private String findDatabaseName(JsonNode source) {

		if (source.isMissingNode()) {
			if (_logger != null)
				_logger.error("Did not found source name.");
			return "";
		} else {
			JsonNode nameNode = source.path("name");
			if (nameNode.isMissingNode()) {
				if (_logger != null)
					_logger.error("Source name is empty.");
				return "";
			} else
				return nameNode.asText();
		}
	}

	/**
	 * Processes tables in the 'views' entry
	 * 
	 * @param source
	 * @param schemaDatabase
	 * @param years
	 * @return false if 'views' entry is missing
	 */
	private boolean findViews(JsonNode source, Database schemaDatabase, int[] years) {
		JsonNode views = source.path("views");
		if (views.isMissingNode() || !views.isArray()) {
			if (_logger != null)
				_logger.error("Did not found 'views' entry or is not an array");
			return false;
		}

		for (JsonNode view : views) {
			Iterator<String> tableNames = view.fieldNames();
			// there is only a single key per object
			String tableName = tableNames.next();
			// do not return here as one view might fail but others work
			Table t = schemaDatabase.findTableByName(tableName);
			if (t != null) {
				if (t.findColumnByName("Bezugsjahr") != null)
					processViewByYear(source, view, tableName, "Bezugsjahr", schemaDatabase, years);
				else
					processView(source, view, tableName, schemaDatabase);
			} else {
				if (_logger != null)
					_logger.warn("Did not found an table entry for '{}' in database '{}'", tableName,
							schemaDatabase.getName());
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
	 * @return false if view could not be processed
	 */
	private boolean processViewByYear(JsonNode source, JsonNode view, String tableName, String yearColumn,
			Database schemaDatabase, int[] years) {
		for (int year : years) {
			_queryCreator.startOrGroup();
			_queryCreator.addWhere(tableName, yearColumn, Integer.toString(year), "=", "NUMERIC");
			_queryCreator.endOrGroup(tableName);
			if (!processView(source, view, tableName, schemaDatabase, Integer.toString(year)))
				return false;
		}
		return true;
	}

	private boolean processView(JsonNode source, JsonNode view, String tableName, Database schemaDatabase,
			String partLabel) {
		_queryCreator.addTable(tableName);
		JsonNode selectParameters = view.path(tableName);
		if (!evaluateColumnEntry(selectParameters, tableName,
				schemaDatabase.findTableByName(tableName).getAllColumns())) {
			if (_logger != null)
				_logger.error(
						"Did not found any columns.\nCheck 'column' field or database schema file for table '{}'.",
						tableName);
			return false;
		}
		evaluateWhereEntry(selectParameters, tableName);
		// extract global conditions from source node
		evaluateWhereEntry(source, tableName);
		evaluateJoinEntry(selectParameters, tableName, schemaDatabase);
		Query query = _queryCreator.buildQuery();
		String qName = schemaDatabase.getName() + "_" + tableName;
		qName += (partLabel.isEmpty()) ? partLabel : "." + partLabel;
		query.setName(qName);
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
	private boolean processView(JsonNode source, JsonNode view, String tableName, Database schemaDatabase) {
		return processView(source, view, tableName, schemaDatabase, "");
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
	private boolean evaluateColumnEntry(JsonNode selectParameters, String tableName, Collection<Column> schemaColumns) {
		JsonNode columns = selectParameters.path("columns");
		if (columns.isMissingNode()) {
			if (schemaColumns != null) {
				for (Column c : schemaColumns)
					_queryCreator.addColumn(tableName, c.getName());
			} else
				return false;
		} else {
			for (JsonNode column : columns) {
				_queryCreator.addColumn(tableName, column.asText());
			}
		}
		return true;
	}

	private void evaluateWhereEntry(JsonNode selectParameters, String tableName) {
		JsonNode wheres = selectParameters.path("where");
		// where entry is optional so skip everything if absent
		if (wheres.isMissingNode())
			return;

		Iterator<String> columns = wheres.fieldNames();
		while (columns.hasNext()) {
			String column = columns.next();
			JsonNode columnConditions = wheres.path(column);
			_queryCreator.startOrGroup();
			for (JsonNode condition : columnConditions) {
				JsonNode valueNode = condition.path("value");
				if (valueNode.isMissingNode()) {
					if (_logger != null)
						_logger.error("Did not found 'value' entry for condition on table '{}.{}'", tableName, column);
					continue;
				}

				JsonNode operatorNode = condition.path("operator");
				if (operatorNode.isMissingNode()) {
					if (_logger != null)
						_logger.error("Did not found 'operator' entry for condition on table '{}.{}'", tableName,
								column);
					continue;
				}

				JsonNode typeNode = condition.path("type");
				if (typeNode.isMissingNode()) {
					if (_logger != null)
						_logger.error("Did not found 'type' entry for condition on table '{}.{}'", tableName, column);
					continue;
				}
				_queryCreator.addWhere(tableName, column, valueNode.asText(), operatorNode.asText(), typeNode.asText());
			}
			_queryCreator.endOrGroup(tableName);
		}
	}

	private void evaluateJoinEntry(JsonNode selectParameters, String leftTable, Database schemaDatabase) {
		JsonNode joinNode = selectParameters.path("join");

		// optional node
		if (joinNode.isMissingNode())
			return;

		for (JsonNode joinEntry : joinNode) {
			JsonNode tableNode = joinEntry.path("table");
			if (tableNode.isMissingNode()) {
				if (_logger != null)
					_logger.error("Did not found 'table' entry for join entry on table '{}'", leftTable);
				continue;
			}
			String table = tableNode.asText();
			JsonNode joinTypeNode = joinEntry.path("type");
			if (joinTypeNode.isMissingNode()) {
				if (_logger != null)
					_logger.error("Did not found 'type' entry for join entry on table '{}'", leftTable);
				continue;
			}

			// this extracts and adds columns that should be also displayed
			// from the joined table
			JsonNode columnNode = joinEntry.path("column");
			if (!columnNode.isMissingNode()) {
				for (JsonNode column : columnNode) {
					if (column.asText().equalsIgnoreCase("*")) {
						for (Column schemaColumn : schemaDatabase.findTableByName(table).getAllColumns()) {
							_queryCreator.addColumn(table, schemaColumn.getName());
						}
					} else
						_queryCreator.addColumn(table, column.asText());
				}
			}

			JsonNode onColumnNode = joinEntry.path("on");
			if (onColumnNode.isMissingNode()) {
				if (_logger != null)
					_logger.error("Did not found 'on' entry for join entry on table '{}'", table);
				continue;
			}

			_queryCreator.addJoin(leftTable, table, onColumnNode.asText(), joinTypeNode.asText());
			evaluateWhereEntry(joinEntry, table);
		}

	}
}
