package de.ingef.eva.constant;

public final class Templates {
	public static final String RAW_COLUMN_DELIMITER = "<<@>>";
	public static final String QUERY_COLUMNS = "help column %s.%s.*";
	public static final String COLUMN_PROCESSING = "coalesce(trim(%s),'')";
	public static final String QUERY_FORMAT = "select\n%s\nfrom %s.%s;";
	public static final String RESTRICTED_QUERY_FORMAT = "select\n%s\nfrom %s.%s\nwhere %s sample 100;";
	public static final String TASK_FORMAT = ".begin export sessions %d;\n.export outfile %s mode record format text;\n%s\n.end export;\n\n";
	public static final String JOB_FORMAT = ".logtable %s.%s;\n.logon %s/%s,%s;\n%s\n.system '%s';\n.logoff;";
	
	public final class Decoding {
		public static final String INVALID_PIDS_QUERY = "select distinct case WHEN CHAR_LENGTH(CAST(PID as VARCHAR(10)))=5 then '00000'||CAST(PID as Char(5)) WHEN CHAR_LENGTH(CAST(PID as VARCHAR(10)))=6 then '0000'||CAST(PID as Char(6)) WHEN CHAR_LENGTH(CAST(PID as VARCHAR(10)))=7 then '000'||CAST(PID as Char(7)) WHEN CHAR_LENGTH(CAST(PID as VARCHAR(10)))=8 then '00'||CAST(PID as Char(8)) WHEN CHAR_LENGTH(CAST(PID as VARCHAR(10)))=9 then '0'||CAST(PID as Char(9)) END as PID from ( select D.*, MAX_Gueltig_BIS from ( select PID, EGK_NR, H2ik from ACC_ADB.AVK_ADB_PID_AKTUELL where EGK_NR in ( select egk_nr from ACC_ADB.AVK_ADB_PID_AKTUELL where h2ik in (%s) and EGK_NR<>'' group by 1 having count(*) >1 ) and h2ik in (%s) ) D Left JOIN ACC_SPECTRUM.AVK_VKS_MENSCH_AKTUELL VKS ON D.H2ik=VKS.H2IK and D.EGK_NR=VKS.EGK_NR and D.PID=VKS.K_MENSCH_ID ) DS QUALIFY RANK() OVER (PARTITION BY EGK_NR ORDER BY MAX_Gueltig_BIS) = 1 ";
		public static final String PID_DECODE_QUERY = "select h2ik, egk_nr, kv_nummer, pid from acc_adb.AVK_ADB_PID_AKTUELL where h2ik in (%s) and kv_nummer not in (select a.kv_nummer from (select kv_nummer, count(*) as mapCount from acc_adb.avk_adb_pid_aktuell where h2ik in (%s) group by kv_nummer having mapCount > 1) a)";
	}
	
	public final class Statistics {		
		public static final String ADB_STATISTICS_FOR_HEMI_HIMI = "SELECT Bezugsjahr, CASE WHEN EXTRACT(MONTH FROM Verordnungsdatum) IN ('01','02','03') THEN 1 WHEN EXTRACT(MONTH FROM Verordnungsdatum) IN ('04','05','06') THEN 2 WHEN EXTRACT(MONTH FROM Verordnungsdatum) IN ('07','08','09') THEN 3 WHEN EXTRACT(MONTH FROM Verordnungsdatum) IN ('10','11','12') THEN 4 END AS Quartal, COUNT(DISTINCT EFN_ID) AS Anz_HeMi_VO FROM ACC_ADB.AVK_ADB_T_${tableSuffix} WHERE h2ik IN (${h2ik}) and Bezugsjahr IN (2010,2011,2012,2013,2014,2015,2016,2017) GROUP BY Bezugsjahr, Quartal ORDER BY Bezugsjahr, Quartal;";
		public static final String ADB_STATISTICS_FOR_KH_FALL = "SELECT Bezugsjahr, Q, SUM(Anz_KH_FAELLE) AS Anz_KH_FAELLE FROM ( SELECT Bezugsjahr, CASE WHEN EXTRACT(MONTH FROM Entlassungsdatum) IN (1,2,3) THEN 1 WHEN EXTRACT(MONTH FROM Entlassungsdatum) IN (4,5,6) THEN 2 WHEN EXTRACT(MONTH FROM Entlassungsdatum) IN (7,8,9) THEN 3 WHEN EXTRACT(MONTH FROM Entlassungsdatum) IN (10,11,12) THEN 4 END AS Q, COUNT(DISTINCT kh_fall_id) AS Anz_KH_FAELLE FROM ACC_ADB.AVK_ADB_T_KH_FALL t1 WHERE h2ik IN (${h2ik}) and Bezugsjahr IN (2010, 2011, 2012,2013,2014,2015,2016,2017) GROUP BY Bezugsjahr, Q ) AS VO GROUP BY Bezugsjahr, Q ORDER BY Bezugsjahr, Q;";
		public static final String ADB_STATISTICS_FOR_ARZT_FALL = "SELECT Bezugsjahr, Behandl_Quartal, COUNT(DISTINCT EFN_ID) AS Anz_EFN FROM ACC_ADB.AVK_ADB_T_ARZT_FALL AF WHERE h2ik in (${h2ik}) and Bezugsjahr IN (2010,2011,2012,2013,2014,2015,2016,2017) AND Vertrags_ID='KV' GROUP BY 1,2 ORDER BY 1,2;";
		public static final String ADB_STATISTICS_FOR_AU_FALL = "SELECT Bezugsjahr, Quartal, SUM(ANZ_AU) AS Anz_AU_Faelle FROM ( SELECT Bezugsjahr, CASE WHEN EXTRACT(MONTH FROM AU_BEGINN) IN ('01','02','03') THEN 1 WHEN EXTRACT(MONTH FROM AU_BEGINN) IN ('04','05','06') THEN 2 WHEN EXTRACT(MONTH FROM AU_BEGINN) IN ('07','08','09') THEN 3 WHEN EXTRACT(MONTH FROM AU_BEGINN) IN ('10','11','12') THEN 4 END as Quartal, COUNT(DISTINCT PID||LANR) AS Anz_AU FROM ACC_ADB.AVK_ADB_T_AU_Fall WHERE h2ik IN (${h2ik}) and Bezugsjahr IN (2010,2011,2012,2013,2014,2015,2016,2017) GROUP BY Bezugsjahr, Quartal ) AS AU GROUP BY Bezugsjahr, Quartal ORDER BY Bezugsjahr, Quartal;";
		public static final String ADB_STATISTICS_FOR_AM_EVO = "SELECT Bezugsjahr, Q, SUM(ANZ_VO) AS Anz_AM_VO FROM(SELECT Bezugsjahr, Verordnungsdatum, CASE WHEN EXTRACT(MONTH FROM Verordnungsdatum) IN (1,2,3) THEN 1 WHEN EXTRACT(MONTH FROM Verordnungsdatum) IN (4,5,6) THEN 2 WHEN EXTRACT(MONTH FROM Verordnungsdatum) IN (7,8,9) THEN 3 WHEN EXTRACT(MONTH FROM Verordnungsdatum) IN (10,11,12) THEN 4 END AS Q, COUNT(DISTINCT PID||PZN||LANR) AS Anz_VO FROM ACC_ADB.AVK_ADB_T_AM_EVO WHERE h2ik IN (${h2ik}) and Bezugsjahr IN (2010,2011, 2012,2013,2014,2015,2016,2017) GROUP BY Bezugsjahr, Verordnungsdatum, Q) AS VO GROUP BY Bezugsjahr, Q ORDER BY Bezugsjahr, Q;";
		
		public static final String ADB_OUTPATIENT_DATA_BY_KV_QUERY = "select coalesce(a.bezugsjahr, 0) as bezugsjahr, coalesce(a.behandl_quartal, 0) as quartal, lukv.kv, lukv.kv_name, coalesce(a.anz_efn, 0) as anz_efn from (select af.bezugsjahr, af.behandl_quartal, af.kv, count(distinct af.efn_id) as anz_efn from acc_adb.avk_adb_t_arzt_fall af, (select top 1 a.bezugsjahr, a.behandl_quartal, a.bezugsjahr - 1 as prevYear from acc_adb.avk_adb_t_arzt_fall a where a.h2ik IN (${h2ik}) and a.vertrags_id = 'KV' order by a.bezugsjahr desc , a.behandl_quartal desc) latest where af.h2ik IN (${h2ik}) and (af.bezugsjahr = latest.bezugsjahr and af.behandl_quartal = latest.behandl_quartal or af.bezugsjahr = latest.prevYear and af.behandl_quartal = latest.behandl_quartal) and af.vertrags_id='kv' group by 1,2,3 ) a right join acc_adb.av_lu_kv lukv on lukv.kv = a.kv where lukv.km6_id <> '' and (lukv.hinweis is null or lukv.hinweis like '%ab%') order by 3;";
		
		public static final String FDB_STATISTICS_BY_TABLE = "select a.bezugsjahr, a.quarter, count(*) from (select a.*, case when substr(cast(a.bezugsjahr as char(8)), 5) >='0101' and substr(cast(a.bezugsjahr as char(8)), 5) <= '0331' then 1 when substr(cast(a.bezugsjahr as char(8)), 5) >='0401' and substr(cast(a.bezugsjahr as char(8)), 5) <= '0630' then 2 when substr(cast(a.bezugsjahr as char(8)), 5) >='0701' and substr(cast(a.bezugsjahr as char(8)), 5) <= '0930' then 3 when substr(cast(a.bezugsjahr as char(8)), 5) >='1001' and substr(cast(a.bezugsjahr as char(8)), 5) <= '1231' then 4 end as quarter from acc_fdb.AVK_FDB_T_${tableSuffix} a) a group by a.bezugsjahr, a.quarter order by a.bezugsjahr, a.quarter asc;";
		public static final String FDB_OUTPATIENT_DATA_BY_KV_QUERY = "select a.bezugsjahr, a.behandl_quartal, a.kv, b.kv_name, count(*) from acc_fdb.AVK_FDB_T_Arzt_Fall a inner join acc_adb.av_lu_kv b on a.kv = b.kv and a.kv <> '' group by a.bezugsjahr, a.behandl_quartal, a.kv, b.kv_name order by a.bezugsjahr, a.behandl_quartal asc";
	}
}
