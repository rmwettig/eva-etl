package de.ingef.eva.dataprocessor;

import java.util.Collection;
import java.util.Map;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class SeparationMapping {
	private final Map<String,String> h2ik2DatasetName;
	
	/**
	 * Searches the dataset name for the given h2ik
	 * @param h2ik
	 * @return empty string if h2ik is not found
	 */
	public String findDatasetName(String h2ik) {
		if(h2ik.contains(h2ik))
			return h2ik2DatasetName.get(h2ik);
		return "";
	}
		
	public Collection<String> getDatasetNames() {
		return h2ik2DatasetName.values();
	}
}
