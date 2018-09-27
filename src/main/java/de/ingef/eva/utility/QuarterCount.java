package de.ingef.eva.utility;

import de.ingef.eva.measures.cci.Quarter;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@Getter
@RequiredArgsConstructor
@ToString
public class QuarterCount {
	private final Quarter quarter;
	private final int count;
	
	@Setter
	private double changeRatio = 0f;
}
