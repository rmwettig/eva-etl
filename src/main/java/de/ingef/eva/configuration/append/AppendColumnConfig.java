package de.ingef.eva.configuration.append;

import de.ingef.eva.etl.transformers.WidoColumn;
import lombok.Getter;
import lombok.ToString;

/**
 * Defines a column from WIDO data
 */
@Getter
@ToString
public class AppendColumnConfig {
	/**
	 * index of the column to be appended
	 */
	private int index;
	/**
	 * selected Wido column
	 * If the same column appears in different sources data of earlier configurations is overwritten.
	 */
	private WidoColumn column;
	
	/**
	 * use this field to use a column only for internal calculations
	 */
	private boolean meta = false;
}
