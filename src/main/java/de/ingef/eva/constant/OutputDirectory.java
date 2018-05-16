package de.ingef.eva.constant;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class OutputDirectory {
	
	public static final Charset DATA_CHARSET = StandardCharsets.UTF_8;
	
	public enum DirectoryType {
		CACHE,
		PRODUCTION,
		REPORT
	}
	
	public static final String CACHE_FILE_EXTENSION = ".csv.gz";
	public static final String OUTPUT_FILE_EXTENSION = ".csv";
}
