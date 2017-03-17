package de.ingef.eva.query;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import de.ingef.eva.constant.Templates;
import de.ingef.eva.utility.Alias;

public class SimpleQueryCreator implements QueryCreator {

	private Alias _alias;

	private String _database;
	private Map<String, Collection<String>> _tables;
	private Map<String, Collection<OrGroup>> _where;
	private Map<String, String> _tableAlias;
	private Collection<Join> _joins;
	private Set<String> _joinRightTables;
	private OrGroup _currentGroup;

	private String _rowStartTerm;
	private String _columnDelimiter;

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

	private class Join {
		private String _leftTable;
		private String _rightTable;
		private String _type;
		private String _primaryColumn;

		public Join(String left, String right, String type, String primary) {
			_leftTable = left;
			_rightTable = right;
			_type = type;
			_primaryColumn = primary;
		}

		public String getLeftTable() {
			return _leftTable;
		}

		public String getRightTable() {
			return _rightTable;
		}

		public String getType() {
			return _type;
		}

		public String getPrimaryColumn() {
			return _primaryColumn;
		}
	}

	public SimpleQueryCreator() {
		this("';ROW_START'", "||';'||");
	}

	public SimpleQueryCreator(String rowStartTerm, String columnDelimiter) {
		_tables = new LinkedHashMap<String, Collection<String>>();
		_joins = new ArrayList<Join>();
		_where = new LinkedHashMap<String, Collection<OrGroup>>();
		_joinRightTables = new HashSet<String>();
		_tableAlias = new HashMap<String, String>();
		_rowStartTerm = rowStartTerm;
		_columnDelimiter = columnDelimiter;
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
		Join joinTerm = new Join(leftTable, rightTable, type, onColumn);
		_joins.add(joinTerm);
		_joinRightTables.add(rightTable);
		createAlias(leftTable);
		createAlias(rightTable);
	}

	@Override
	public void addWhere(String table, String column, String value, String operator, String type) {
		String modifiedValue = value;
		if (type.equalsIgnoreCase("STRING"))
			modifiedValue = "'" + value + "'";
		if (type.equalsIgnoreCase("COLUMN")) {
			// dot must be escaped since it means 'any character' in a regular
			// expression
			String[] content = value.split("\\.");
			String tableValue = content[0];
			String columnValue = content[1];
			modifiedValue = (_tableAlias.containsKey(tableValue))
					? String.format("%s.%s", _tableAlias.get(tableValue), columnValue)
					: String.format("%s.%s.%s", _database, tableValue, columnValue);
		}
		Where whereClause = new Where(column, modifiedValue, operator);
		if (_currentGroup != null)
			_currentGroup.addTerm(whereClause);
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
		_alias.reset();
	}

	private Query buildSelect() {
		String selectFormat = "%s.%s.%s";
		String aliasedFormat = "%s.%s";
		StringBuilder selectClause = new StringBuilder();

		selectClause.append("select\n\t");
		selectClause.append(_rowStartTerm);
		selectClause.append(_columnDelimiter);
		selectClause.append("\n\t");
		StringBuilder fromClause = new StringBuilder();
		fromClause.append("from\n\t");
		Collection<String> qColumns = new ArrayList<String>();

		// iterate over all tables for which columns should be appear in the
		// result set
		for (String table : _tables.keySet()) {
			boolean hasAlias = _tableAlias.containsKey(table);
			// do not add tables that appear as right tables in an 'join' clause
			if (!_joinRightTables.contains(table)) {
				fromClause.append(_database + "." + table);
				if (hasAlias)
					fromClause.append(" " + _tableAlias.get(table));
				fromClause.append(",");
				fromClause.append("\n\t");
			}
			Collection<String> columns = _tables.get(table);
			qColumns.addAll(columns);
			for (String column : columns) {
				String col = (hasAlias) ? String.format(aliasedFormat, _tableAlias.get(table), column)
						: String.format(selectFormat, _database, table, column);
				selectClause.append(String.format(Templates.COLUMN_PROCESSING, col));
				selectClause.append(_columnDelimiter);
				selectClause.append("\n\t");
			}
		}

		selectClause.replace(selectClause.lastIndexOf(_columnDelimiter), selectClause.length(), "\n");
		fromClause.replace(fromClause.lastIndexOf(","), fromClause.length(), "\n");

		selectClause.append(fromClause);

		if (_joins.size() > 0)
			selectClause.append(buildJoin());
		if (_where.size() > 0)
			selectClause.append(buildWhere());

		selectClause.append(";");

		return new SimpleQuery(selectClause.toString(), qColumns);
	}

	private StringBuilder buildJoin() {
		String join = "%s join %s\non %s=%s";

		StringBuilder joinClause = new StringBuilder();

		for (Join j : _joins) {
			String left = (_tableAlias.containsKey(j.getLeftTable()))
					? String.format("%s.%s", _tableAlias.get(j._leftTable), j.getPrimaryColumn())
					: String.format("%s.%s.%s", _database, j.getLeftTable(), j.getPrimaryColumn());
			boolean hasRightTableAlias = _tableAlias.containsKey(j.getRightTable());
			String right = (hasRightTableAlias)
					? String.format("%s.%s", _tableAlias.get(j.getRightTable()), j.getPrimaryColumn())
					: String.format("%s.%s.%s", _database, j.getRightTable(), j.getPrimaryColumn());
			String joinTable = (hasRightTableAlias)
					? String.format("%s.%s %s", _database, j.getRightTable(), _tableAlias.get(j.getRightTable()))
					: String.format("%s.%s", _database, j.getRightTable());

			String clause = String.format(join, j.getType(), joinTable, left, right);
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
