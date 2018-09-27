package de.ingef.eva.measures.cci;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor 
public class QuarterScoreResult {
	private final Quarter start;
	private final Quarter end;
	private final int weight;
}