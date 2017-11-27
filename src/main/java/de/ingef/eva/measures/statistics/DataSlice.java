package de.ingef.eva.measures.statistics;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public enum DataSlice {
	AU_FALL("AU_Fall"),
	KH_FALL("KH_Fall"),
	HEMI_EVO("HeMi_EVO"),
	HIMI_EVO("HiMi_EVO"),
	ARZT_FALL("Arzt_Fall"),
	AM_EVO("AM_EVO");
	
	private final String label;
}
