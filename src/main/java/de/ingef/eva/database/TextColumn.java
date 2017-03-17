package de.ingef.eva.database;

public class TextColumn implements Column {

	private String _name;
	private String _type;

	public TextColumn(String name, String type) {
		_name = name;
		_type = type;
	}

	public TextColumn(String name) {
		this(name, "");
	}

	@Override
	public String getName() {
		return _name;
	}

	@Override
	public String getType() {
		return _type;
	}

}
