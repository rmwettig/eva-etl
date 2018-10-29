package de.ingef.eva.configuration.decoding;

import java.util.List;

import lombok.Getter;

/**
 * Decoding file creation configuration
 */
@Getter
public class DecodingConfig {
    /** dataset name **/
	private String name;
	/** h2iks associated with the dataset **/
	private List<String> h2iks;
}
