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
	STANAME("STANAME"),
	PACK_SIZE("PACKGR\u00d6E"),
	ATC("ATC_AI")
	;
	
	private final String label;
}
