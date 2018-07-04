package de.ingef.eva.database;

import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.Set;

import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * Represents a table of a database schema.
 * Equality is only based on its name.
 * @author Martin.Wettig
 *
 */
@EqualsAndHashCode(exclude = {"_columns"})
@ToString
public class Table {

	private String _name = "";
	private Set<Column> _columns = new LinkedHashSet<Column>();

	public Table(String name) {
		_name = name;
	}

	public void addColumn(Column c) {
		if(!_columns.contains(c))
			_columns.add(c);
	}

	/**
	 * @return empty optional if column with given name was not found
	 */
	public Optional<Column> findColumnByName(String name) {
		return _columns.stream().filter(column -> column.getName().equalsIgnoreCase(name)).findFirst();
	}

	public Set<Column> getAllColumns() {
		return _columns;
	}

	public String getName() {
		return _name;
	}

}
