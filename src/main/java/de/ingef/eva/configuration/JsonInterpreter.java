package de.ingef.eva.configuration;

import java.util.Collection;

import com.fasterxml.jackson.databind.JsonNode;

import de.ingef.eva.query.Query;

public interface JsonInterpreter {
	public Collection<Query> interpret(JsonNode node);
}
