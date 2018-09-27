package de.ingef.eva.configuration.cci;

import java.util.List;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public class CCISource {
	private final String db;
	private final String dataset;
	private final List<String> iks;
	private final String flag;
}