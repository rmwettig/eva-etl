package de.ingef.eva.constant;

public final class Templates {
	public static final String QUERY_COLUMNS = "help column %s.%s.*";
	public static final String COLUMN_PROCESSING = "coalesce(trim(%s),'')";
	public static final String QUERY_FORMAT = "select\n%s\nfrom %s.%s;";
	public static final String RESTRICTED_QUERY_FORMAT = "select\n%s\nfrom %s.%s\nwhere %s sample 100;";
	public static final String TASK_FORMAT = ".begin export sessions %d;\n.export outfile %s mode record format text;\n%s\n.end export;\n\n";
	public static final String JOB_FORMAT = ".logtable %s.%s;\n.logon %s/%s,%s;\n%s\n.system '%s';\n.logoff;";
	
	public final class Decoding {
		public static final String INVALID_PIDS_QUERY = "select distinct case WHEN CHAR_LENGTH(CAST(PID as VARCHAR(10)))=5 then '00000'||CAST(PID as Char(5)) WHEN CHAR_LENGTH(CAST(PID as VARCHAR(10)))=6 then '0000'||CAST(PID as Char(6)) WHEN CHAR_LENGTH(CAST(PID as VARCHAR(10)))=7 then '000'||CAST(PID as Char(7)) WHEN CHAR_LENGTH(CAST(PID as VARCHAR(10)))=8 then '00'||CAST(PID as Char(8)) WHEN CHAR_LENGTH(CAST(PID as VARCHAR(10)))=9 then '0'||CAST(PID as Char(9)) END as PID from ( select D.*, MAX_Gueltig_BIS from ( select PID, EGK_NR, H2ik from ACC_ADB.AVK_ADB_PID_AKTUELL where EGK_NR in ( select egk_nr from ACC_ADB.AVK_ADB_PID_AKTUELL where h2ik='%s' and EGK_NR<>'' group by 1 having count(*) >1 ) and h2ik='%s' ) D Left JOIN ACC_SPECTRUM.AVK_VKS_MENSCH_AKTUELL VKS ON D.H2ik=VKS.H2IK and D.EGK_NR=VKS.EGK_NR and D.PID=VKS.K_MENSCH_ID ) DS QUALIFY RANK() OVER (PARTITION BY EGK_NR ORDER BY MAX_Gueltig_BIS) = 1 ";
		public static final String PID_DECODE_QUERY = "select h2ik, egk_nr, kv_nummer, pid from acc_adb.AVK_ADB_PID_AKTUELL where h2ik = '%s' and kv_nummer not in (select a.kv_nummer from (select kv_nummer, count(*) as mapCount from acc_adb.avk_adb_pid_aktuell where h2ik='%s' group by kv_nummer having mapCount > 1) a)";
	}
}
