package de.ingef.eva.processor;

import com.fasterxml.jackson.databind.JsonNode;

import de.ingef.eva.query.QueryCreator;

public interface SourceProcessor {
	public StringBuilder process(JsonNode node, QueryCreator creator);
}
