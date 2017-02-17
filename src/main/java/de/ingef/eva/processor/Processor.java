package de.ingef.eva.processor;

public interface Processor<T> {
	T process(T value);
}
