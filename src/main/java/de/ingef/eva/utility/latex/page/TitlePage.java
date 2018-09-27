package de.ingef.eva.utility.latex.page;

import de.ingef.eva.utility.latex.Figure;
import de.ingef.eva.utility.latex.LatexNode;
import lombok.RequiredArgsConstructor;

/**
 * Title page with logo and no page number
 * 
 * @author Martin.Wettig
 *
 */
@RequiredArgsConstructor
public class TitlePage implements LatexNode {

	private final Figure corporateLogo;
	
	@Override
	public String render() {
		return new StringBuilder()
				.append("\\begin{titlepage}\n")
				.append("\\maketitle\n")
				.append("\\thispagestyle{empty}\n")
				.append(corporateLogo.render())
				.append("\n")
				.append("\\end{titlepage}\n")
				.toString();
	}

}
