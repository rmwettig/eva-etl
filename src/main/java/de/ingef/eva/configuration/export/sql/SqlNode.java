package de.ingef.eva.configuration.export.sql;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.annotation.JsonTypeIdResolver;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Base class for json sql mapping
 */
@Getter
@RequiredArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.CUSTOM, include = JsonTypeInfo.As.PROPERTY, property="type")
@JsonTypeIdResolver(SqlNodeTypeIdResolver.class)
public class SqlNode {
	private final SqlNodeType type;
}
