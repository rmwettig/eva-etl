package de.ingef.eva.configuration.export;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonTypeName;

import de.ingef.eva.configuration.export.sql.DynamicYearSlice;
import de.ingef.eva.configuration.export.sql.SqlNode;
import de.ingef.eva.configuration.export.sql.SqlNodeType;
import de.ingef.eva.configuration.export.sql.YearSliceNode;
import de.ingef.eva.query.Query;
import de.ingef.eva.query.creation.QueryCreator;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter @Setter
@JsonTypeName(value="DB")
@ToString
public class SourceConfig extends SqlNode {
	private String db;
	private String datasetName;
	private List<ViewConfig> views = Collections.emptyList();
	private List<WhereConfig> where = Collections.emptyList();
	private YearSliceNode yearSlice = new DynamicYearSlice("bezugsjahr", 3);
	
	public SourceConfig() {
		super(SqlNodeType.DB);
	}
		
	public List<Query> traverse(QueryCreator builder) {
		builder.setDatabase(db);
		builder.setDatasetName(datasetName);
		builder.setYearSlice(yearSlice);
		return views
				.stream()
				.flatMap(table -> table.traverse(this, builder, where).stream())
				.collect(Collectors.toList());
	};
}
