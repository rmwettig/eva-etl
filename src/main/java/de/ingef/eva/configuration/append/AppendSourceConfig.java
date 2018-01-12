package de.ingef.eva.configuration.append;

import java.nio.file.Path;
import java.util.List;

import lombok.Getter;
import lombok.ToString;

@Getter
@ToString
public class AppendSourceConfig {
	
	/**
	 * zero-based column index of the identifier column
	 */
	private int keyColumnIndex;
	
	/**
	 * columns that should be added
	 */
	private List<AppendColumnConfig> columns;
	
	/**
	 * path to the content file without header
	 */
	private Path file;
}
