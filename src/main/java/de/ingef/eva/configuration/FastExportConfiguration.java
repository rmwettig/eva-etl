package de.ingef.eva.configuration;

import java.util.Calendar;

import com.fasterxml.jackson.databind.JsonNode;

public class FastExportConfiguration {
	private String _logDatabase = "sb_hri";
	private String _logTable = "tmp_dumplog_";
	private String _rowPrefix = "ROW_START;";
	private String _jobFilename = "fejob.fx";
	private String _postDumpAction = "";
	private int _sessions = 2;

	public FastExportConfiguration(JsonNode root) {
		JsonNode feNode = root.path("fastexport");
		if (!feNode.isMissingNode()) {
			JsonNode node = feNode.path("logDatabase");
			if (node.isMissingNode())
				System.out.println("Missing entry 'logDatabase'. Default: " + _logDatabase);
			else
				_logDatabase = node.asText();

			node = feNode.path("logTable");
			if (node.isMissingNode()) {
				_logTable += String.format("tmp_dumplog_%d_%d_%d", Calendar.getInstance().get(Calendar.YEAR),
						Calendar.getInstance().get(Calendar.MONTH), Calendar.getInstance().get(Calendar.DAY_OF_MONTH));
				System.out.println("Missing entry 'logTable'. Default: " + _logTable);
			} else
				_logTable = node.asText();

			node = feNode.path("rowPrefix");
			if (node.isMissingNode())
				System.out.println("Missing entry 'rowPrefix'. Default: " + _rowPrefix);
			else
				_rowPrefix = node.asText();

			node = feNode.path("sessions");
			if (node.isMissingNode())
				System.out.println("Missing entry 'rowPrefix'. Default: " + _sessions);
			else
				_sessions = node.asInt();

			node = feNode.path("jobFilename");
			if (node.isMissingNode())
				System.out.println("Missing entry 'jobFilename'. Default: " + _jobFilename);
			else
				_jobFilename = node.asText();

			node = feNode.path("postDumpAction");
			_postDumpAction = (!node.isMissingNode()) ? node.asText() : "";
		}
	}

	public String getLogDatabase() {
		return _logDatabase;
	}

	public String getLogTable() {
		return _logTable;
	}

	public String getRowPrefix() {
		return _rowPrefix;
	}

	public int getSessions() {
		return _sessions;
	}

	public String getJobFilename() {
		return _jobFilename;
	}

	public String getPostDumpAction() {
		return _postDumpAction;
	}
}
