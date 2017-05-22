package de.ingef.eva.error;

/**
 * Custom exception for indicating a failure of a DataTable instance method call
 * @author Martin Wettig
 *
 */
public class DataTableOperationException extends Exception {
	private static final long serialVersionUID = 1L;
	
	public DataTableOperationException(String message, Throwable cause) {
		super(message, cause);
	}
}
