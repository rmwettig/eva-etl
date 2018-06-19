package de.ingef.eva.utility.latex.alignment;

import de.ingef.eva.utility.latex.LatexNode;
import de.ingef.eva.utility.latex.table.BorderMode;
import lombok.RequiredArgsConstructor;

/**
 * Specify alignment and cell border of table cells
 * 
 * @author Martin.Wettig
 *
 */
@RequiredArgsConstructor
public class AlignmentOption implements LatexNode {

	private final Anchor position;
	private final BorderMode borderMode;
	
	@Override
	public String render() {
		switch(borderMode) {
			case LEFT:
				return "|" + position.getPosition();
			case RIGHT:
				return  position.getPosition() + "|";
			case BOTH:
				return "|" + position.getPosition() + "|";
			default:
				return position.getPosition();
		}
	}

}
