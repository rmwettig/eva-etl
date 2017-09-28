package de.ingef.eva.query.creation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import de.ingef.eva.database.Column;
import de.ingef.eva.database.Database;
import de.ingef.eva.database.DatabaseHost;
import de.ingef.eva.database.Table;
import de.ingef.eva.query.Query;
import de.ingef.eva.utility.Alias;
import lombok.Getter;

public class SimpleQueryCreator implements QueryCreator {

	private Alias _alias;
	private DatabaseHost _schema;
	
	private String _database;
	private Map<String, Collection<String>> _tables;
	private Map<String, Collection<OrGroup>> _where;
	private Map<String, String> _tableAlias;
	private Collection<Join> _joins;
	private Set<String> _joinRightTables;
	private OrGroup _currentGroup;

	private String _rowStartTerm;
	
	private class Where {
		private String _column;
		private String _value;
		private String _operator;

		public Where(String column, String value, String operator) {
			_column = column;
			_value = value;
			_operator = operator;
		}

		public String getColumn() {
			return _column;
		}

		public String getValue() {
			return _value;
		}

		public String getOperator() {
			return _operator;
		}
	}

	private class OrGroup {
		private Collection<Where> _terms;

		public OrGroup() {
			_terms = new ArrayList<Where>();
		}

		public void addTerm(Where w) {
			_terms.add(w);
		}

		public Collection<Where> getTerms() {
			return _terms;
		}
	}

	@Getter
	private class Join {
		private String leftTable;
		private String rightTable;
		private String type;
		private Collection<String> primaryColumns;
		private String subQuery;
		
		public Join(String leftTable, String rightTable, String type, String primaryColumn) {
			this.leftTable = leftTable;
			this.rightTable = rightTable;
			this.type = type;
			this.primaryColumns = new ArrayList<String>();
			this.primaryColumns.add(primaryColumn);
		}
		
		public void addPrimaryColumn(String onColumn) {
			primaryColumns.add(onColumn);
		}
	}
		
	public SimpleQueryCreator(DatabaseHost schema, String rowStartTerm) {
		_tables = new LinkedHashMap<String, Collection<String>>();
		_joins = new ArrayList<Join>();
		_where = new LinkedHashMap<String, Collection<OrGroup>>();
		_joinRightTables = new HashSet<String>();
		_tableAlias = new HashMap<String, String>();
		_rowStartTerm = rowStartTerm.isEmpty() ? "" : "'" + rowStartTerm + "'";
		_schema = schema;
	}

	@Override
	public void setDatabase(String name) {
		_database = name;
	}

	@Override
	public void addTable(String name) {
		if (!_tables.containsKey(name))
			_tables.put(name, new ArrayList<String>());
		createAlias(name);
	}

	@Override
	public void addColumn(String table, String name) {
		if (_tables.containsKey(table))
			_tables.get(table).add(name);
		else {
			Collection<String> list = new ArrayList<String>();
			list.add(name);
			_tables.put(table, list);
		}
	}

	@Override
	public void addJoin(String leftTable, String rightTable, String onColumn, String type) {
		for(Join j : _joins) {
			if(j.getLeftTable().equalsIgnoreCase(leftTable) && j.getRightTable().equalsIgnoreCase(rightTable)){
				j.addPrimaryColumn(onColumn);
				return;
			}
		}
		Join joinTerm = new Join(leftTable, rightTable, type, onColumn);
		_joins.add(joinTerm);
		_joinRightTables.add(rightTable);
		createAlias(leftTable);
		createAlias(rightTable);
	}
	
	/**
	 * Creates a where constraint
	 * @param table column owning table
	 * @param column name of the constraint column
	 * @param value constraint value
	 * @param type type of the value
	 */
	@Override
	public void addWhere(String table, String column, String value, String operator, String type) {
		String modifiedValue = value;
		if (type.equalsIgnoreCase("STRING"))
			modifiedValue = "'" + value + "'";
		if (type.equalsIgnoreCase("COLUMN")) {
			//if column was specified using table.column
			if(!value.isEmpty() && value.contains("\\.")) {
				// dot must be escaped since it means 'any character' in a regular expression
				String[] content = value.split("\\.");
				String tableValue = content[0];
				String columnValue = content[1];
				modifiedValue = (_tableAlias.containsKey(tableValue))
						? String.format("%s.%s", _tableAlias.get(tableValue), columnValue)
						: String.format("%s.%s.%s", _database, tableValue, columnValue);
			} else {
				modifiedValue = _tableAlias.get(table) + "." + column;
			}
			
		}
		Where whereClause = new Where(column, modifiedValue, operator);
		if (_currentGroup != null)
			_currentGroup.addTerm(whereClause);
	}
	
	/**
	 * Creates a where clause term that compares two columns
	 * @param leftTable main table specified in 'from' clause
	 * @param leftColumn main table column to be compared
	 * @param rightTable subquery table
	 * @param rightColumn subquery table column
	 * @param operator comparison operator
	 */
	@Override
	public void addWhere(String leftTable, String leftColumn, String rightTable, String rightColumn, String operator, String type) {
		String withAlias = "%s.%s";
		String noAlias = "%s" + withAlias;
 
		
	}
	
	@Override
	public void startOrGroup() {
		_currentGroup = new OrGroup();
	};

	@Override
	public void endOrGroup(String table) {
		if (_where.containsKey(table)) {
			_where.get(table).add(_currentGroup);
			_currentGroup = null;
		} else {
			Collection<OrGroup> conditions = new ArrayList<OrGroup>();
			conditions.add(_currentGroup);
			_where.put(table, conditions);
		}
	};

	@Override
	public Query buildQuery() {

		Query query = buildSelect();

		clearCreator();
		return query;
	}

	private void clearCreator() {
		_tables.clear();
		_joins.clear();
		_where.clear();
		_tableAlias.clear();
		if(_alias != null) _alias.reset();
	}

	private Query buildSelect() {
		String selectFormat = "%s.%s.%s";
		String aliasedFormat = "%s.%s";
		StringBuilder selectClause = new StringBuilder();
		selectClause.append("select\n\t");
		if(_rowStartTerm != null && !_rowStartTerm.isEmpty()) {
			selectClause.append(_rowStartTerm);
			selectClause.append(",\n\t");
		}
		StringBuilder fromClause = new StringBuilder();
		fromClause.append("from\n\t");
		List<Column> qColumns = new ArrayList<>();
		
		// iterate over all tables for which columns should be appear in the
		// result set
		for (String table : _tables.keySet()) {
			boolean hasAlias = _tableAlias.containsKey(table);
			// do not add tables that appear as right tables in an 'join' clause
			if (!_joinRightTables.contains(table)) {
				fromClause.append(_database + "." + table);
				if (hasAlias)
					fromClause.append(" " + _tableAlias.get(table));
				fromClause.append(",\n\t");
			}
			Collection<String> columns = _tables.get(table);
			qColumns.addAll(createAnnotatedColumns(table, columns));
			
			for (String column : columns) {
				String col = (hasAlias) ? String.format(aliasedFormat, _tableAlias.get(table), column)
						: String.format(selectFormat, _database, table, column);
				selectClause.append(col);
				selectClause.append(",\n\t");
			}
		}

		selectClause.replace(selectClause.lastIndexOf(","), selectClause.length(), "\n");
		fromClause.replace(fromClause.lastIndexOf(","), fromClause.length(), "\n");

		selectClause.append(fromClause);

		if (_joins.size() > 0)
			selectClause.append(buildJoin());
		if (_where.size() > 0)
			selectClause.append(buildWhere());

		selectClause.append(";");
		
		return new SimpleQuery(selectClause.toString(), qColumns);
	}

	private List<Column> createAnnotatedColumns(String tableName, Collection<String> columnNames) {
		List<Column> header = new ArrayList<>(columnNames.size());
		Database db = _schema.findDatabaseByName(_database);
		Table table = db.findTableByName(tableName);
		for(String columnName : columnNames) {
			Column column = table.findColumnByName(columnName);
			if(column == null) continue;
			header.add(column);
		}
		
		return header;
	}
	
	private StringBuilder buildJoin() {
		
		StringBuilder joinClause = new StringBuilder();

		for (Join j : _joins) {
			String leftOnConditionPart = (_tableAlias.containsKey(j.getLeftTable()))
					? _tableAlias.get(j.leftTable)
					: String.format("%s.%s", _database, j.getLeftTable());
			boolean hasRightTableAlias = _tableAlias.containsKey(j.getRightTable());
			//if there is an alias use the access schema alias.column
			//otherwise use schema db.table.column
			String rightOnConditionPart;
			String rightTable = j.getRightTable();
			String joinTable;
			//if rightTable has a bracket it is a subquery
			if(rightTable.startsWith("(")) {
				String alias = _tableAlias.get(rightTable);
				rightOnConditionPart = alias;
				joinTable = rightTable + " " + alias;
			} else {
				rightOnConditionPart = (hasRightTableAlias)
						? _tableAlias.get(j.getRightTable())
						: String.format("%s.%s", _database, j.getRightTable());
				joinTable = (hasRightTableAlias)
						? String.format("%s.%s %s", _database, j.getRightTable(), _tableAlias.get(j.getRightTable()))
						: String.format("%s.%s", _database, j.getRightTable());
			}			
			
			StringBuilder ons = new StringBuilder();
			
			for(String c : j.getPrimaryColumns()) {
				ons.append("(");
				ons.append(leftOnConditionPart + "." + c + "=" + rightOnConditionPart + "." + c);
				ons.append(") and");
			}
			ons.delete(ons.lastIndexOf(")")+1, ons.length());
			String join = "%s join %s\non %s";
			String clause = String.format(join, j.getType(), joinTable, ons.toString());
			joinClause.append(clause);
			joinClause.append('\n');
		}

		return joinClause;
	}

	private StringBuilder buildWhere() {
		StringBuilder whereClause = new StringBuilder();
		whereClause.append("where\n\t");
		String format = "%s %s %s";
		for (String table : _where.keySet()) {
			String alias = null;
			if (_tableAlias.containsKey(table))
				alias = _tableAlias.get(table);

			for (OrGroup group : _where.get(table)) {
				whereClause.append("(");
				for (Where w : group.getTerms()) {
					String selector = (alias != null) ? String.format("%s.%s", alias, w.getColumn())
							: String.format("%s.%s.%s", _database, table, w.getColumn());
					String term = String.format(format, selector, w.getOperator(), w.getValue());
					whereClause.append(term);
					whereClause.append(" or ");
				}
				// remove trailing or
				whereClause.delete(whereClause.length() - 4, whereClause.length());
				whereClause.append(")\n\tand\n\t");
			}
		}
		whereClause.delete(whereClause.lastIndexOf(")") + 1, whereClause.length());

		return whereClause;
	}

	@Override
	public void setAliasFactory(Alias alias) {
		_alias = alias;
	}

	private void createAlias(String name) {
		if (!_tableAlias.containsKey(name) && _alias != null)
			_tableAlias.put(name, _alias.findNextAlias());
	}
}
