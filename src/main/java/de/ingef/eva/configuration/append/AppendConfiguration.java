package de.ingef.eva.configuration.append;

import java.nio.file.Path;
import java.util.List;

import lombok.Getter;

@Getter
public class AppendConfiguration {
	private TransformerType mode;
	private AppendOrder order;
	private String targetDb;
	private String targetTable;
	private String valueName;
	private String value;
	/**
	 * column name that holds identifiers
	 */
	private String keyColumn;
	private List<AppendSourceConfig> sources;
	private List<String> excludeTables;
	/*
	 * configuration for dynamic column transformer
	 */
	private Path file;
	private List<String> keyColumns;
}
