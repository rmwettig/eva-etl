package de.ingef.eva.query.creation;

import de.ingef.eva.query.Query;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class SimpleQuery implements Query {
	private String name;
	private String query;
	private String dbName;
	private String tableName;
	private String sliceName;
	private String datasetName;
}
