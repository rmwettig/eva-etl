package de.ingef.eva.tasks;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Base class for specific tasks
 * @author Martin.Wettig
 *
 * @param <T>
 */
@Getter
@RequiredArgsConstructor
public abstract class Task<T> {
	private final String name;
	private final String description;	
	public abstract T execute();
}
