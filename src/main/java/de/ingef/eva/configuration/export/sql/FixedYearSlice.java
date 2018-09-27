package de.ingef.eva.configuration.export.sql;

import java.time.LocalDate;

import com.fasterxml.jackson.annotation.JsonTypeName;

import lombok.Getter;
import lombok.Setter;

@Getter @Setter
@JsonTypeName(value="FIXED_YEAR_SLICE")
public class FixedYearSlice extends YearSliceNode {

	private final int startYear;
	private final int endYear;
	
	public FixedYearSlice() {
		this("", LocalDate.now().getYear() - 1, LocalDate.now().getYear());
	}
	
	public FixedYearSlice(String yearColumn, int start, int end) {
		super(SqlNodeType.FIXED_YEAR_SLICE, yearColumn);
		startYear = start;
		endYear = end;
	}

	@Override
	protected int calculateStartYear() {
		return startYear;
	}
	
	@Override
	protected int calculateEndYear() {
		return endYear;
	}

}
