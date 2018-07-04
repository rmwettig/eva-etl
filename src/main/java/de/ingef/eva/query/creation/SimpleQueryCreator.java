package de.ingef.eva.query.creation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import de.ingef.eva.configuration.export.JoinType;
import de.ingef.eva.configuration.export.WhereOperator;
import de.ingef.eva.configuration.export.WhereType;
import de.ingef.eva.configuration.export.sql.YearSliceNode;
import de.ingef.eva.database.Column;
import de.ingef.eva.database.Database;
import de.ingef.eva.database.DatabaseHost;
import de.ingef.eva.database.Table;
import de.ingef.eva.query.Query;
import de.ingef.eva.utility.Alias;
import lombok.Builder;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Singular;

public class SimpleQueryCreator implements QueryCreator {

	private Alias aliaser = new Alias(20);
	private DatabaseHost schema;
	
	private String database;
	private String datasetName;
	private Map<String, String> tableAlias;
	private OrGroup.OrGroupBuilder currentGroup;

	private List<String> selectedColumns = new ArrayList<>();
	private List<String> selectedTables = new ArrayList<>();
	private List<JoinInfo> joinColumns = new ArrayList<>();
	private List<OrGroup> conditions = new ArrayList<>();
	private List<OrGroup> globalConditions = new ArrayList<>();
	private YearSliceNode yearSlice;
	
	@Getter
	@RequiredArgsConstructor
	private class JoinInfo {
		private final JoinType type;
		private final String mainTable;
		private final String joinTable;
		private final List<String> joinColumns;
		
		public String render(String database, Map<String, String> aliases) {
			return new StringBuilder()
					.append(type.getName())
					.append(" join ")
					.append(database + "." + joinTable + " " + aliases.get(joinTable) + " ")
					.append("on ")
					.append(
						joinColumns
							.stream()
							.map(column -> aliases.get(mainTable) + "." + column + "=" + aliases.get(joinTable) + "." + column)
							.collect(Collectors.joining(" and "))
					)
					.toString();
		}
	}
	
	@Getter
	@RequiredArgsConstructor
	private class Where {
		private final String table;
		private final String column;
		private final List<String> values;
		private final WhereOperator operator;
		private final WhereType type;
		
		public List<String> render(Map<String, String> tableAliases) {
			return values
					.stream()
					.map(v -> {
						return new StringBuilder()
							.append(tableAliases.get(table) + "." + column)
							.append(" " + operator.getSymbol() + " ")
							.append(encode(v))
							.toString();
					})
					.collect(Collectors.toList());
		}
		
		public String encode(String value) {
			switch (type) {
				case STRING:
					return "'" + value + "'";
				default:
					return value;
			}
		}
	}

	@Getter
	@Builder
	private static class OrGroup {
		@Singular
		private List<Where> _terms;
		
		public String render(Map<String, String> tableAliases) {
			return new StringBuilder()
					.append("(")
					.append(_terms.stream().flatMap(w -> w.render(tableAliases).stream()).collect(Collectors.joining(" or ")))
					.append(")")
					.toString();
		}
	}
		
	public SimpleQueryCreator(DatabaseHost schema) {
		tableAlias = new HashMap<String, String>();
		this.schema = schema;
	}

	@Override
	public void setDatabase(String name) {
		database = name;
	}

	/**
	 * adds a table to from clause. The table will be aliased automatically.
	 * This method must be called before {@code addColumn}
	 */
	@Override
	public void addTable(String name) {
		selectedTables.add(name);
		if(!tableAlias.containsKey(name))
			tableAlias.put(name, createOrFindAlias(name));
	}

	/**
	 * adds a column to the select clause. The table alias will be appended automatically.
	 * Register the table by calling {@code addTable} before adding columns
	 */
	@Override
	public void addColumn(String table, String name) {
		selectedColumns.add(createOrFindAlias(table) + "." + name);
	}

	@Override
	public void addJoin(String leftTable, String rightTable, List<String> onColumns, JoinType type) {
		joinColumns.add(new JoinInfo(type, leftTable, rightTable, onColumns));
	}
	
	/**
	 * Creates a where constraint
	 * @param table column owning table
	 * @param column name of the constraint column
	 * @param value constraint value
	 * @param type type of the value
	 */
	@Override
	public void addWhere(String table, String column, List<String> values, WhereOperator operator, WhereType type) {
		if (currentGroup != null)
			currentGroup._term(new Where(table, column, values, operator, type));
	}
		
	@Override
	public void startOrGroup() {
		currentGroup = OrGroup.builder();
	};

	@Override
	public void endOrGroup() {
		conditions.add(currentGroup.build());
		currentGroup = null;
	}

	@Override
	public List<Query> buildQueries() {
		List<Query> query = buildSelect();
		clearCreator();
		
		return query;
	}

	private void clearCreator() {
		selectedTables.clear();
		selectedColumns.clear();
		joinColumns.clear();
		conditions.clear();
		globalConditions.clear();
		tableAlias.clear();
		if(aliaser != null) aliaser.reset();
	}

	private List<Query> buildSelect() {
		String selectClause = createColumnSelect();
		String fromClause = createFromClause();
		String joinClause = createJoins();
		String whereClause = createWhereConditions();
		StringBuilder baseQuery =
				new StringBuilder()
					.append(selectClause)
					.append(" ")
					.append(fromClause)
					.append(" ")
					.append(joinClause)
					.append(" ")
					.append(whereClause);
		if(yearSlice != null && tableHasYearColumn(yearSlice.getColumn()))
			return createSlicedQueries(baseQuery, !whereClause.isEmpty());
		else
			return createUnslicedQuery(baseQuery);
	}

	private boolean tableHasYearColumn(String yearColumnName) {
		Database db = schema.findDatabaseByName(database);
		if(db == null) return false;
		Table t = db.findTableByName(selectedTables.get(0));
		if(t == null) return false;
		Column c = t.findColumnByName(yearColumnName);
		return c != null;
	}

	private List<Query> createUnslicedQuery(StringBuilder baseQuery) {
		return Collections.singletonList(createFinalQuery(baseQuery.append(";").toString(), ""));
	}

	private SimpleQuery createFinalQuery(String baseQuery, String sliceName) {
		return SimpleQuery
			.builder()
			.dbName(database)
			.datasetName(datasetName)
			.query(baseQuery)
			.tableName(selectedTables.get(0))
			.sliceName(sliceName)
			.build();
	}

	private List<Query> createSlicedQueries(StringBuilder baseQuery, boolean whereClauseExists) {
		List<Integer> years = yearSlice.calculateYearRange();
		List<String> queries = new ArrayList<>(years.size());
		for(int year : years) {
			queries.add(
						new StringBuilder(baseQuery)
						.append(whereClauseExists ? " and " : "where ")
						.append(
							new Where(
									selectedTables.get(0),
									yearSlice.getColumn(),
									Collections.singletonList(Integer.toString(year)),
									WhereOperator.EQUAL,
									WhereType.NUMERIC)
							.render(tableAlias).get(0)
						)
						.append(";")
						.toString()
			);
		}
		
		return IntStream
				.range(0, years.size())
				.mapToObj(index -> createFinalQuery(queries.get(index), Integer.toString(years.get(index))))
				.collect(Collectors.toList());
	}

	private String createFromClause() {
		return new StringBuilder()
						.append("from ")
						.append(
							selectedTables
							.stream()
							.map(table -> database + "." + table + " " + createOrFindAlias(table))
							.collect(Collectors.joining(", "))
						)
						.toString();
	}

	private String createColumnSelect() {
		return new StringBuilder()
						.append("select ")
						.append(selectedColumns.stream().collect(Collectors.joining(", ")))
						.toString();
	}

	private String createJoins() {
		return joinColumns
				.stream()
				.map(joinInfo -> joinInfo.render(database, tableAlias))
				.collect(Collectors.joining(" "));
	}
	
	private String createWhereConditions() {
		if(conditions.isEmpty() && globalConditions.isEmpty())
			return "";
		StringBuilder whereClause =
				new StringBuilder()
					.append(conditions.stream().map(condition -> condition.render(tableAlias)).collect(Collectors.joining(" and ")));
		if(globalConditions != null && !globalConditions.isEmpty()) {
			if(whereClause.length() > 0)
				whereClause.append(" and ");
			whereClause
				.append(globalConditions.stream().map(condition -> condition.render(tableAlias)).collect(Collectors.joining(" and ")));
		}
		whereClause.insert(0, "where ");
		return whereClause.toString();
	}

	private String createOrFindAlias(String name) {
		if (tableAlias.containsKey(name))
			return tableAlias.get(name);
		
		String alias = aliaser.findNextAlias();
		tableAlias.put(name, alias);
		return alias;
	}

	/**
	 * use all columns that exist in the database.
	 * Call {@code addTable} before calling this method.
	 */
	@Override
	public void addAllKnownColumns(String table) {
		addAllKnownColumns(table, Collections.emptyList());
	}

	/**
	 * use all columns that exist in the database except for those specified.
	 * Call {@code addTable} before calling this method.
	 */
	@Override
	public void addAllKnownColumns(String table, List<String> excludeColumns) {
		Database db = schema.findDatabaseByName(database);
		if(db != null) {
			Table t = db.findTableByName(table);
			if(t != null) {
				Set<String> exclude = new HashSet<>(excludeColumns);
				t.getAllColumns()
					.stream()
					.filter(column -> !exclude.contains(column.getName()))
					.forEach(column -> addColumn(table, column.getName()));
			}
		}
	}

	@Override
	public void setYearSlice(YearSliceNode slice) {
		yearSlice = slice;
	}

	@Override
	public void setDatasetName(String name) {
		datasetName = name;
	}
	
	@Override
	public void addGlobalWhere(String table, String column, List<String> values, WhereOperator symbol, WhereType name) {
		globalConditions.add(OrGroup
				.builder()
				._term(new Where(table, column, values, symbol, name))
				.build()
		);
	}
}
