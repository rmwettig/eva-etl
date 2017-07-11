package de.ingef.eva.configuration;

import com.fasterxml.jackson.databind.JsonNode;

import de.ingef.eva.constant.Templates;
import de.ingef.eva.error.InvalidConfigurationException;
import lombok.Getter;

@Getter
public class FastExportConfiguration {
	private String logDatabase;// = "sb_hri";
	private String logTable;// = "tmp_dumplog_";
	private String rowPrefix;// = "ROW_START;";
	private String jobFilename;// = "fejob.fx";
	private String postDumpAction;// = "";
	private int sessions;// = 2;
	private String rawColumnDelimiter;
		
	public static FastExportConfiguration loadFromJson(JsonNode root) throws InvalidConfigurationException {
		// json field names
		String FASTEXPORT = "fastexport";
		String LOGDATABASE = "logDatabase";
		String LOGTABLE = "logTable";
		String ROWPREFIX = "rowPrefix";
		String SESSIONS = "sessions";
		String JOBFILE = "jobFilename";
		String POSTDUMPACTION = "postDumpAction";
		String RAW_DELIMITER = "rawDelimiter";
		
		FastExportConfiguration config = new FastExportConfiguration();
		JsonNode feNode = root.path(FASTEXPORT);
		
		if(feNode.isMissingNode()) throw new InvalidConfigurationException(String.format(ErrorMessage.MISSING_FIELD, FASTEXPORT));
		JsonNode node = feNode.path(LOGDATABASE);
		
		if (node.isMissingNode()) throw new InvalidConfigurationException(String.format(ErrorMessage.MISSING_FIELD, LOGDATABASE));
		config.logDatabase = node.asText();
		
		node = feNode.path("logTable");
		if (node.isMissingNode()) throw new InvalidConfigurationException(String.format(ErrorMessage.MISSING_FIELD, LOGTABLE));
		config.logTable = node.asText();

		node = feNode.path("rowPrefix");
		if (node.isMissingNode()) throw new InvalidConfigurationException(String.format(ErrorMessage.MISSING_FIELD, ROWPREFIX));
		config.rowPrefix = node.asText();

		node = feNode.path("sessions");
		if (node.isMissingNode()) throw new InvalidConfigurationException(String.format(ErrorMessage.MISSING_FIELD, SESSIONS));
		config.sessions = node.asInt();

		node = feNode.path("jobFilename");
		if (node.isMissingNode()) throw new InvalidConfigurationException(String.format(ErrorMessage.MISSING_FIELD, JOBFILE));
		config.jobFilename = node.asText();

		node = feNode.path(POSTDUMPACTION);
		config.postDumpAction = (!node.isMissingNode()) ? node.asText() : "";
		
		node = feNode.path(RAW_DELIMITER);
		config.rawColumnDelimiter = node.asText(Templates.RAW_COLUMN_DELIMITER);
		
		return config;
	}
}
