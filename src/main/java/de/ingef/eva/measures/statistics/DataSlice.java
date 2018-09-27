package de.ingef.eva.measures.statistics;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public enum DataSlice {
	AU_FALL("AU_Fall", "AU-F\u00e4lle"),
	KH_FALL("KH_Fall", "F\u00e4lle TP4a"),
	HEMI_EVO("HeMi_EVO", "VO TP5 HeMi"),
	HIMI_EVO("HiMi_EVO", "VO TP5 HiMi"),
	ARZT_FALL("Arzt_Fall", "EFN TP1"),
	AM_EVO("AM_EVO", "VO TP3");
	
	private final String table;
	private final String label;
}
