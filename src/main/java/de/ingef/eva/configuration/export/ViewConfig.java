package de.ingef.eva.configuration.export;

import java.util.List;

import lombok.Getter;
import lombok.ToString;

@Getter
@ToString
public class ViewConfig {
	private String name;
	private List<String> columns;
	private List<String> excludeColumns;
	private List<WhereConfig> where;
	private List<JoinConfig> join;
	private boolean latest;
	private String timestamp;
	private String idColumn;
}
