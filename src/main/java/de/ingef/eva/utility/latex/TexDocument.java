package de.ingef.eva.utility.latex;

import java.util.List;

import lombok.Builder;
import lombok.Singular;

/**
 * Latex document root object
 * 
 * @author Martin.Wettig
 *
 */
@Builder
public class TexDocument {
	
	@Singular
	private List<PreambleEntry> preambleEntries;
	@Singular
	private List<LatexNode> elements;
	
	public String renderDocument() {
		StringBuilder document = new StringBuilder();
		preambleEntries.stream().map(e -> e.render() + "\n").forEach(line -> document.append(line));
		document.append("\\begin{document}\n");
		elements.stream().map(e -> e.render() + "\n").forEach(line -> document.append(line));
		document.append("\\end{document}");
		return document.toString();
	}
}
