package de.ingef.eva.query;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public class SimpleQueryCreator implements QueryCreator {
	
	private String _database;
	private Map<String, Collection<String>> _tables;
	private Map<String, Collection<OrGroup>> _where;
	private Collection<Join> _joins;
	private Set<String> _joinRightTables;
	private OrGroup _currentGroup;
	
	private class Where{
		private String _column;
		private String _value;
		private String _operator;
		
		public Where(String column, String value, String operator){
			_column = column;
			_value = value;
			_operator = operator;
		}
		
		public String getColumn(){
			return _column;
		}
		public String getValue(){
			return _value;
		}
		
		public String getOperator(){
			return _operator;
		}
	}
	
	private class OrGroup{
		private Collection<Where> _terms;
		public OrGroup(){
			_terms = new ArrayList<Where>();
		}
		public void addTerm(Where w){
			_terms.add(w);
		}
		public Collection<Where> getTerms(){
			return _terms;
		}
	}
	
	
	private class Join{
		private String _leftTable;
		private String _rightTable;
		private String _type;
		private String _primaryColumn;
		
		public Join(String left, String right, String type, String primary)
		{
			_leftTable = left;
			_rightTable = right;
			_type = type;
			_primaryColumn = primary;
		}
		
		public String getLeftTable()
		{
			return _leftTable;
		}
		public String getRightTable()
		{
			return _rightTable;
		}
		public String getType()
		{
			return _type;
		}
		public String getPrimaryColumn()
		{
			return _primaryColumn;
		}
	}
	
	public SimpleQueryCreator() {
		_tables = new LinkedHashMap<String, Collection<String>>();
		_joins = new ArrayList<Join>();
		_where = new LinkedHashMap<String, Collection<OrGroup>>();
		_joinRightTables = new HashSet<String>();
	}
	
	@Override
	public void setDatabase(String name) {
		_database = name;
	}

	@Override
	public void addTable(String name) {
		if(!_tables.containsKey(name))
			_tables.put(name, new ArrayList<String>());
	}

	@Override
	public void addColumn(String table, String name) {
		if(_tables.containsKey(table))
			_tables.get(table).add(name);
		else
		{
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
	}

	@Override
	public void addWhere(String table, String column, String value, String operator, String type) {
		String modifiedValue = value;
		if(type.equalsIgnoreCase("STRING"))
			modifiedValue = "'"+value+"'";
		Where whereClause = new Where(column, modifiedValue, operator);
		if(_currentGroup != null)
			_currentGroup.addTerm(whereClause);
	}

	@Override
	public void startOrGroup(){
		_currentGroup = new OrGroup();
	};
	@Override
	public void endOrGroup(String table){
		if(_where.containsKey(table))
		{
			_where.get(table).add(_currentGroup);
			_currentGroup = null;
		}
		else
		{
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
	}

	private Query buildSelect()
	{
		String selectFormat = "%s.%s.%s";
		
		StringBuilder selectClause = new StringBuilder();
		selectClause.append("select ");
		
		StringBuilder fromClause = new StringBuilder();
		fromClause.append("from ");
		Collection<String> qColumns = new ArrayList<String>();
		
		//iterate over all tables for which columns should be appear in the result set
		for(String table : _tables.keySet())
		{
			//do not add tables that appear as right tables in an 'join' clause
			if(!_joinRightTables.contains(table))
			{
				fromClause.append(_database+"."+table);
				fromClause.append(",");
			}
			Collection<String> columns =_tables.get(table);
			qColumns.addAll(columns);
			for(String column : columns)
			{
				selectClause.append(String.format(selectFormat, _database, table, column));
				selectClause.append(",");
			}
		}
		
		selectClause.setCharAt(selectClause.length() - 1, '\n');
		if(fromClause.charAt(fromClause.length()-1) == ',')
			fromClause.setCharAt(fromClause.length() - 1, '\n');
		
		selectClause.append(fromClause);
		
		if(_joins.size() > 0)
			selectClause.append(buildJoin());
		if(_where.size() > 0)
			selectClause.append(buildWhere());
		
		selectClause.append(";");
		
		return new SimpleQuery(selectClause.toString(), qColumns);
	}
	
	private StringBuilder buildJoin()
	{
		String join = "%s join %s.%s\non %s.%s.%s=%s.%s.%s";
		StringBuilder joinClause = new StringBuilder();
		
		for(Join j : _joins)
		{
			String clause = String.format(
					join,
					j.getType(),
					_database, j.getRightTable(),
					_database, j.getLeftTable(), j.getPrimaryColumn(),
					_database, j.getRightTable(), j.getPrimaryColumn());
			joinClause.append(clause);
			joinClause.append('\n');
		}
			
		return joinClause;
	}
	
	private StringBuilder buildWhere()
	{
		StringBuilder whereClause = new StringBuilder();
		whereClause.append("where ");
		String format = "%s.%s.%s %s %s";
		for(String table : _where.keySet())
		{
			for(OrGroup group : _where.get(table))
			{
				whereClause.append("(");
				for(Where w : group.getTerms())
				{
					String term = String.format(format, 
							_database, table, w.getColumn(),
							w.getOperator(),
							w.getValue()
							);
					whereClause.append(term);
					whereClause.append(" or ");
				}
				//remove trailing or
				whereClause.delete(whereClause.length() - 4, whereClause.length());
				whereClause.append(") and ");
			}
		}
		whereClause.delete(whereClause.length() - 5, whereClause.length());

		return whereClause;
	}
}
