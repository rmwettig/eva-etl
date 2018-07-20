package de.ingef.eva.error;

public class TaskExecutionException extends RuntimeException {

	private static final long serialVersionUID = 1L;
	
	public TaskExecutionException(String message) {
		this(message, null);
	}
	
	public TaskExecutionException(String message, Throwable cause) {
		super(message, cause);
	}

}
