package de.ingef.eva.etl;

import java.time.LocalDate;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public class DateRange {
	private final LocalDate start;
	private final LocalDate end;
}
