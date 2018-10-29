package de.ingef.eva.database;

import java.util.Collection;
import java.util.HashSet;
import java.util.Optional;

import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * Database representation
 */
@EqualsAndHashCode(exclude = {"_tables"})
@ToString
public class Database {

	private Collection<Table> _tables = new HashSet<Table>();
	private String _name;

	public Database(String name) {
		_name = name;
	}

	/**
	 * @return empty optional if table was not found
	 */
	public Optional<Table> findTableByName(String name) {
		return _tables.stream().filter(table -> table.getName().equalsIgnoreCase(name)).findFirst();
	}

	public Collection<Table> getAllTables() {
		return _tables;
	}

	public String getName() {
		return _name;
	}

	public void addTable(Table t) {
		if(!_tables.contains(t))
			_tables.add(t);
	}

}
