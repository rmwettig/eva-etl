package de.ingef.eva.utility.latex.alignment;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Position on page
 * 
 * @author Martin.Wettig
 *
 */
@Getter
@RequiredArgsConstructor
public enum Anchor {
	BOTTOM("b"),
	HERE("h"),
	CENTER("c");
	
	private final String position;
}