package de.ingef.eva.configuration.export.sql;

import java.time.LocalDate;

import com.fasterxml.jackson.annotation.JsonTypeName;

import lombok.Getter;
import lombok.Setter;

/**
 * Calculates a year span. The starting year is the current year minus the specified number of previous years.
 * @author Martin.Wettig
 *
 */
@Getter @Setter
@JsonTypeName(value="DYNAMIC_YEAR_SLICE")
public class DynamicYearSlice extends YearSliceNode {

	private final int numberOfPreviousYears;
	
	public DynamicYearSlice() {
		this("", 1);
	}
	
	public DynamicYearSlice(String yearColumn, int previousYears) {
		super(SqlNodeType.DYNAMIC_YEAR_SLICE, yearColumn);
		numberOfPreviousYears = previousYears;
	}
	
	@Override
	protected int calculateStartYear() {
		return LocalDate.now().getYear() - numberOfPreviousYears;
	}
	
	@Override
	protected int calculateEndYear() {
		return LocalDate.now().getYear();
	}
}
