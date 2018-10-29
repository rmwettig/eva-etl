package de.ingef.eva.configuration.export.sql;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import lombok.Getter;

/**
 * Base class for different year slicing methods
 */
@Getter
public abstract class YearSliceNode extends SqlNode {
	
	private final String column;
	
	public YearSliceNode(SqlNodeType type, String yearColumn) {
		super(type);
		column = yearColumn;
	}
	
	public List<Integer> calculateYearRange() {
		return IntStream
				.rangeClosed(calculateStartYear(), calculateEndYear())
				.boxed()
				.collect(Collectors.toList());
	}
	
	protected abstract int calculateStartYear();
	protected abstract int calculateEndYear();
}
