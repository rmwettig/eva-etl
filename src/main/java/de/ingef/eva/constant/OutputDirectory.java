package de.ingef.eva.constant;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class OutputDirectory {
	/** unprocessed data **/
	public static final String RAW = "raw";
	/** preprocessed data **/
	public static final String CLEAN = "clean";
	/** final data usable for production **/
	public static final String PRODUCTION = "production";
	
	public static final Charset DATA_CHARSET = StandardCharsets.UTF_8;
}
