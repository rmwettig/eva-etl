package de.ingef.eva.configuration.append;

/**
 * Determines which transformer is used
 * @author martin.wettig
 *
 */
public enum TransformerType {
	/**
	 * append a given value to all rows
	 */
	STATIC,
	/**
	 * append columns from wido stamm and supplement file
	 */
	DDD,
	/**
	 * This transformer assesses the BS_NR and maps the first two BS_NR characters onto the major KV id
	 */
	BS_TO_KV,
	/**
	 * maps apothekenik onto category
	 */
	APO_TYPE,
	/**
	 * creates a transformer that appends a SHA-256 hash
	 */
	PID_HASH,
	/**
	 * append columns from file when the specified key column matches
	 */
	DYNAMIC,
	/**
	 * calculate a start date based on end date and days
	 */
	START_DATE,
	/**
	 * quick fix for https://lyo-pgitl01.spectrumk.ads/eva4/dokumentation/issues/262
	 */
	FIX_DATES
}
