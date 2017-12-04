package de.ingef.eva.configuration.append;

/**
 * Determines which transformer is used
 * @author martin.wettig
 *
 */
public enum TransformerType {
	/**
	 * STATIC means that a given value is appended to all rows
	 */
	STATIC,
	/**
	 * DYNAMIC means that the user can provide a file with mappings
	 * and only rows that match a given key are extended with values from the file
	 */
	DDD
}
