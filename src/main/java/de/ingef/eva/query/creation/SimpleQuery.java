package de.ingef.eva.query.creation;

import de.ingef.eva.query.Query;
import lombok.Builder;
import lombok.Data;

/**
 * Sql query with meta data
 */
@Data
@Builder
public class SimpleQuery implements Query {
	private String name;
	private String query;
	private String dbName;
	private String tableName;
	private String sliceName;
	private String datasetName;
	
	public String getDescription() {
		StringBuilder description =
			new StringBuilder()
				.append("DB: ")
				.append(dbName)
				.append(", Table: ")
				.append(tableName);
		if(datasetName != null && !datasetName.isEmpty())
			description.append(", Dataset: ").append(datasetName);
		if(sliceName != null && !sliceName.isEmpty())
			description.append(", Slice: ").append(sliceName);
		description.append("Sql: ").append(query);
		return description.toString();
	}
}
