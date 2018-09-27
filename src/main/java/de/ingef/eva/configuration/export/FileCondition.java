package de.ingef.eva.configuration.export;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonTypeName;

import de.ingef.eva.configuration.export.sql.SqlNodeType;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;

/**
 * Uses values from file as values for condition in where clause for the specified column.
 * The condition file must be headerless and must have only one value per line
 * @author Martin.Wettig
 *
 */
@JsonTypeName(value="WHERE_FILE")
@Getter @Setter
@Log4j2
public class FileCondition extends WhereConfig {

	private Path conditionFile;
	
	public FileCondition() {
		super(SqlNodeType.WHERE_FILE);
	}
	public FileCondition(String columnName, WhereOperator op, WhereType type) {
		super(SqlNodeType.WHERE_FILE, columnName, op, type);
	}
	
	@Override
	protected List<String> prepareConditionValues() {
		try {
			return Files.newBufferedReader(conditionFile).lines().collect(Collectors.toList());
		} catch (IOException e) {
			log.error("Could not read conditions from file '{}'. {}", conditionFile, e);
			return Collections.emptyList();
		}
	}

}
