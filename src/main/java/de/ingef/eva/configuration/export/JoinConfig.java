package de.ingef.eva.configuration.export;

import java.util.List;

import lombok.Getter;
import lombok.ToString;

@Getter
@ToString
public class JoinConfig {
	private String table;
	private List<String> column;
	private JoinType type;
	private List<String> on;
	private List<WhereConfig> where;
}
