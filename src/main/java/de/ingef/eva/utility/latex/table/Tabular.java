package de.ingef.eva.utility.latex.table;

import java.util.List;
import java.util.stream.Collectors;

import de.ingef.eva.utility.latex.LatexNode;
import de.ingef.eva.utility.latex.alignment.AlignmentOption;
import lombok.Builder;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Singular;

/**
 * Table-like latex object
 * 
 * @author Martin.Wettig
 *
 */
@Builder
public class Tabular implements LatexNode {

	private TabularType type;
	private List<AlignmentOption> columnOptions;
	@Singular
	private List<TableRow> rows;
	
	@Getter
	@RequiredArgsConstructor
	public enum TabularType {
		TABULAR("tabular"),
		LONGTABLE("longtable");
		
		private final String typeName;
	}
	
	@Override
	public String render() {
		StringBuilder tabular = new StringBuilder();
		tabular.append(String.format("\\begin{%s}{%s}\n", type.getTypeName(), createColumnLayout()));
		String rowContent =
				rows
					.stream()
					.map(row -> row.render())
					.collect(Collectors.joining("\n"));
		return tabular
				.append(rowContent)
				.append("\n")
				.append(String.format("\\end{%s}", type.getTypeName()))
				.toString();
	}

	private String createColumnLayout() {
		return columnOptions
				.stream()
				.map(option -> option.render())
				.collect(Collectors.joining());
	}
}
