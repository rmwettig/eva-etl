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

/**
 * Model class for a database from which data is exported
 * @author Martin.Wettig
 *
 */
@Getter @Setter
@JsonTypeName(value="DB")
@ToString
public class SourceConfig extends SqlNode {
	/**
	 * Name of the database, e.g. 'ACC_ADB'
	 */
	private String db;
	/**
	 * Name of the dataset to which the exported data belongs to
	 */
	private String datasetName;
	/**
	 * Tables that are exported
	 */
	private List<ViewConfig> views = Collections.emptyList();
	/**
	 * Global where conditions that are applied to all tables
	 */
	private List<WhereConfig> where = Collections.emptyList();
	/**
	 * Years to be exported. By default, the current year plus three previous years are exported.
	 */
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
