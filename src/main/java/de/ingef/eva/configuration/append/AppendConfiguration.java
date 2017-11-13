package de.ingef.eva.configuration.append;

import lombok.Getter;

@Getter
public class AppendConfiguration {
	private AppendMode mode;
	private AppendOrder order;
	private String targetDb;
	private String targetTable;
	private String valueName;
	private String value;
	private String source;
	private String keyColumn;
}
