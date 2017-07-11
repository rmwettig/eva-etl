package de.ingef.eva.error;

public class QueryExecutionException extends Exception {
	private static final long serialVersionUID = 1L;
	
	public QueryExecutionException(String message) {
		this(message, null);
	}
	
	public QueryExecutionException(String message, Throwable cause) {
		super(message, cause);
	}
}
