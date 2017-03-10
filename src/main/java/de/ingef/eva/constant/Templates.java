package de.ingef.eva.constant;

public final class Templates 
{
	public static final String QUERY_COLUMNS = "help table %s.%s";
	public static final String COLUMN_PROCESSING = "coalesce(trim(%s),'')";
	public static final String QUERY_FORMAT = "select\n%s\nfrom %s.%s;";
	public static final String RESTRICTED_QUERY_FORMAT = "select\n%s\nfrom %s.%s\nwhere %s sample 100;";
	public static final String TASK_FORMAT = ".begin export sessions %d;\n.export outfile %s mode record format text;\n%s\n.end export;\n\n";
	public static final String JOB_FORMAT = ".logtable %s.%s;\n.logon %s/%s,%s;\n%s\n.system '%s';\n.logoff;";
}
