package de.ingef.eva.utility.latex;

import java.nio.file.Path;

import de.ingef.eva.utility.latex.alignment.Anchor;
import lombok.RequiredArgsConstructor;

/**
 * Figure environment that creates a centered figure
 * 
 * @author Martin.Wettig
 *
 */
@RequiredArgsConstructor
public class Figure implements LatexNode {
	
	/**
	 * position on page
	 */
	private final Anchor position;
	/**
	 * path to figure
	 */
	private final Path imagePath;
	/**
	 * figure scale
	 */
	private final float scale;
	
	@Override
	public String render() {
		return new StringBuilder()
				.append(String.format("\\begin{figure}[%s]\n", position.getPosition()))
				.append("\\centering\n")
				.append(String.format("\\includegraphics[scale=%f]{%s}\n", scale, imagePath.toString()))
				.append("\\end{figure}\n")
				.toString();
	}
}
