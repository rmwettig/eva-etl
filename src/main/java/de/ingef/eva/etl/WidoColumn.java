package de.ingef.eva.etl;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public enum WidoColumn {
	ADMISSION_DATE("MARKTZUGANG"),
	RETIREMENT_DATE("AHDATUM"),
	METOO("METOO"),
	APPFORM("APPFORM"),
	BIOSIMILAR("BIOSIMILAR"),
	ORPHAN("ORPHAN"),
	GENERIC("GENERIKENN"),
	DDDPK("DDD"),
	STANAME("STANAME")
	;
	
	private final String label;
}
