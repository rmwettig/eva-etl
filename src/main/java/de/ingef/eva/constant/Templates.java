package de.ingef.eva.constant;

/**
 * Sql query templates
 */
public final class Templates {

	public static final String QUERY_COLUMNS = "help column %s.%s.*";
	public static final String QUERY_FORMAT = "select\n%s\nfrom %s.%s;";
	public static final String RESTRICTED_QUERY_FORMAT = "select\n%s\nfrom %s.%s\nwhere %s sample 100;";
	public static final String TASK_FORMAT = ".begin export sessions %d;\n.export outfile %s mode record format text;\n%s\n.end export;\n\n";
	public static final String JOB_FORMAT = ".logtable %s.%s;\n.logon %s/%s,%s;\n%s\n.system '%s';\n.logoff;";
	
	public final class Decoding {
		public static final String INVALID_PIDS_QUERY = "select distinct "
				+ "case WHEN CHAR_LENGTH(CAST(PID as VARCHAR(10)))=5 then '00000'||CAST(PID as Char(5)) "
				+ "WHEN CHAR_LENGTH(CAST(PID as VARCHAR(10)))=6 then '0000'||CAST(PID as Char(6)) "
				+ "WHEN CHAR_LENGTH(CAST(PID as VARCHAR(10)))=7 then '000'||CAST(PID as Char(7)) "
				+ "WHEN CHAR_LENGTH(CAST(PID as VARCHAR(10)))=8 then '00'||CAST(PID as Char(8)) "
				+ "WHEN CHAR_LENGTH(CAST(PID as VARCHAR(10)))=9 then '0'||CAST(PID as Char(9)) "
				+ "END as PID "
				+ "from ("
					+ "select D.*, MAX_Gueltig_BIS "
					+ "from ("
					+ "select PID, EGK_NR, H2ik "
					+ "from ACC_ADB.AVK_ADB_PID_AKTUELL "
					+ "where EGK_NR in ("
					+ "select egk_nr "
					+ "from ACC_ADB.AVK_ADB_PID_AKTUELL "
					+ "where h2ik in (%s) "
					+ "and EGK_NR<>'' "
					+ "group by 1 "
					+ "having count(*) >1) and "
					+ "h2ik in (%s)) D "
				+ "Left JOIN ACC_SPECTRUM.AVK_VKS_MENSCH_AKTUELL VKS "
				+ "ON D.H2ik=VKS.H2IK and "
				+ "D.EGK_NR=VKS.EGK_NR and "
				+ "D.PID=VKS.K_MENSCH_ID ) DS "
				+ "QUALIFY RANK() OVER (PARTITION BY EGK_NR ORDER BY MAX_Gueltig_BIS) = 1 ";
		public static final String PID_DECODE_QUERY = "select h2ik, egk_nr, kv_nummer, pid "
				+ "from acc_adb.AVK_ADB_PID_AKTUELL "
				+ "where h2ik in (%s);";
	}
	
	public final class Statistics {		
		public static final String ADB_STATISTICS_FOR_HEMI_HIMI = "SELECT Bezugsjahr, " +
					"CASE WHEN EXTRACT(MONTH FROM Verordnungsdatum) IN ('01','02','03') THEN 1 " +
					"WHEN EXTRACT(MONTH FROM Verordnungsdatum) IN ('04','05','06') THEN 2 " +
					"WHEN EXTRACT(MONTH FROM Verordnungsdatum) IN ('07','08','09') THEN 3 " + 
					"WHEN EXTRACT(MONTH FROM Verordnungsdatum) IN ('10','11','12') THEN 4 END AS Quartal, " +
					"COUNT(DISTINCT EFN_ID) AS Anz_HeMi_VO " +
				"FROM ACC_ADB.AVK_ADB_T_${tableSuffix} " +
				"WHERE h2ik IN (${h2iks}) and " + 
					"Bezugsjahr IN (${years}) " +
				"GROUP BY Bezugsjahr, Quartal " +
				"ORDER BY Bezugsjahr, Quartal;";
		public static final String ADB_STATISTICS_FOR_KH_FALL = "SELECT Bezugsjahr, " + 
					"Q, " +
					"SUM(Anz_KH_FAELLE) AS Anz_KH_FAELLE " + 
				"FROM ( " +
					"SELECT Bezugsjahr, " +
						"CASE WHEN EXTRACT(MONTH FROM Entlassungsdatum) IN (1,2,3) THEN 1 " +
						"WHEN EXTRACT(MONTH FROM Entlassungsdatum) IN (4,5,6) THEN 2 " +
						"WHEN EXTRACT(MONTH FROM Entlassungsdatum) IN (7,8,9) THEN 3 " +
						"WHEN EXTRACT(MONTH FROM Entlassungsdatum) IN (10,11,12) THEN 4 " +
						"END AS Q, " +
						"COUNT(DISTINCT kh_fall_id) AS Anz_KH_FAELLE " +
					"FROM ACC_ADB.AVK_ADB_T_KH_FALL t1 " + 
					"WHERE h2ik IN (${h2iks}) and " +
						"Bezugsjahr IN (${years}) " + 
					"GROUP BY Bezugsjahr, Q ) AS VO " +
				"GROUP BY Bezugsjahr, Q " +
				"ORDER BY Bezugsjahr, Q;";
		public static final String ADB_STATISTICS_FOR_ARZT_FALL = "SELECT Bezugsjahr, " +
					"Behandl_Quartal, " +
					"COUNT(DISTINCT EFN_ID) AS Anz_EFN " +
				"FROM ACC_ADB.AVK_ADB_T_ARZT_FALL AF " + 
				"WHERE h2ik in (${h2iks}) and " +
					"Bezugsjahr IN (${years}) AND " +
					"Vertrags_ID='KV' " +
				"GROUP BY 1,2" +
				"ORDER BY 1,2;";
		public static final String ADB_STATISTICS_FOR_AU_FALL = "SELECT Bezugsjahr, "
				+ "Quartal, "
				+ "SUM(ANZ_AU) AS Anz_AU_Faelle "
				+ "FROM ( "
					+ "SELECT Bezugsjahr, "
						+ "CASE WHEN EXTRACT(MONTH FROM LEISTUNGS_BEGINN) IN ('01','02','03') THEN 1 "
						+ "WHEN EXTRACT(MONTH FROM LEISTUNGS_BEGINN) IN ('04','05','06') THEN 2 "
						+ "WHEN EXTRACT(MONTH FROM LEISTUNGS_BEGINN) IN ('07','08','09') THEN 3 "
						+ "WHEN EXTRACT(MONTH FROM LEISTUNGS_BEGINN) IN ('10','11','12') THEN 4 END as Quartal, "
						+ "COUNT(DISTINCT PID||LEISTUNGS_BEGINN) AS Anz_AU "
					+ "FROM ACC_ADB.AVK_ADB_T_AU_Fall_LEIST "
					+ "WHERE h2ik IN (${h2iks}) and "
						+ "Bezugsjahr IN (${years}) "
					+ "GROUP BY Bezugsjahr, Quartal ) AS AU "
				+ "GROUP BY Bezugsjahr, Quartal "
				+ "ORDER BY Bezugsjahr, Quartal;";
		public static final String ADB_STATISTICS_FOR_AM_EVO = "SELECT Bezugsjahr, "
				+ "Q, "
				+ "SUM(ANZ_VO) AS Anz_AM_VO "
				+ "FROM ("
					+ "SELECT Bezugsjahr, "
						+ "Verordnungsdatum, "
						+ "CASE WHEN EXTRACT(MONTH FROM Verordnungsdatum) IN (1,2,3) THEN 1 "
						+ "WHEN EXTRACT(MONTH FROM Verordnungsdatum) IN (4,5,6) THEN 2 "
						+ "WHEN EXTRACT(MONTH FROM Verordnungsdatum) IN (7,8,9) THEN 3 "
						+ "WHEN EXTRACT(MONTH FROM Verordnungsdatum) IN (10,11,12) THEN 4 END AS Q, "
						+ "COUNT(DISTINCT PID||PZN||LANR) AS Anz_VO "
					+ "FROM ACC_ADB.AVK_ADB_T_AM_EVO "
					+ "WHERE h2ik IN (${h2iks}) and "
						+ "Bezugsjahr IN (${years}) "
					+ "GROUP BY Bezugsjahr, "
						+ "Verordnungsdatum, Q) AS VO "
				+ "GROUP BY Bezugsjahr, Q "
				+ "ORDER BY Bezugsjahr, Q;";
		
		public static final String ADB_OUTPATIENT_DATA_BY_KV_QUERY = "select coalesce(a.bezugsjahr, 0) as bezugsjahr, "
				+ "coalesce(a.behandl_quartal, 0) as quartal, "
				+ "lukv.kv, "
				+ "lukv.kv_name, "
				+ "coalesce(a.anz_efn, 0) as anz_efn "
				+ "from ("
					+ "select af.bezugsjahr, "
						+ "af.behandl_quartal, "
						+ "af.kv, "
						+ "count(distinct af.efn_id) as anz_efn "
					+ "from acc_adb.avk_adb_t_arzt_fall af, "
						+ "(select top 1 a.bezugsjahr, "
						+ "a.behandl_quartal, "
						+ "a.bezugsjahr - 1 as prevYear "
						+ "from acc_adb.avk_adb_t_arzt_fall a "
						+ "where a.h2ik IN (${h2iks}) and "
							+ "a.vertrags_id = 'KV' "
						+ "order by a.bezugsjahr desc, "
							+ "a.behandl_quartal desc) latest "
						+ "where af.h2ik IN (${h2iks}) and "
							+ "(af.bezugsjahr = latest.bezugsjahr and af.behandl_quartal = latest.behandl_quartal or "
							+ "af.bezugsjahr = latest.prevYear and af.behandl_quartal = latest.behandl_quartal) and "
							+ "af.vertrags_id='kv' "
						+ "group by 1,2,3 ) a "
				+ "right join acc_adb.av_lu_kv lukv "
				+ "on lukv.kv = a.kv "
				+ "where lukv.km6_id <> '' and "
					+ "(lukv.hinweis is null or lukv.hinweis like '%ab%') "
				+ "order by 3;";
		
		public static final String FDB_STATISTICS_FOR_HEMI_HIMI = "SELECT Bezugsjahr, " +
				"CASE WHEN EXTRACT(MONTH FROM Verordnungsdatum) IN ('01','02','03') THEN 1 " +
				"WHEN EXTRACT(MONTH FROM Verordnungsdatum) IN ('04','05','06') THEN 2 " +
				"WHEN EXTRACT(MONTH FROM Verordnungsdatum) IN ('07','08','09') THEN 3 " + 
				"WHEN EXTRACT(MONTH FROM Verordnungsdatum) IN ('10','11','12') THEN 4 END AS Quartal, " +
				"COUNT(DISTINCT EFN_ID) AS Anz_HeMi_VO " +
			"FROM ACC_FDB.AVK_FDB_T_${tableSuffix} " +
			"WHERE flag_${flag} = 1 and " + 
				"Bezugsjahr IN (${years}) " +
			"GROUP BY Bezugsjahr, Quartal " +
			"ORDER BY Bezugsjahr, Quartal;";
		
		public static final String FDB_STATISTICS_FOR_KH_FALL = "SELECT Bezugsjahr, " + 
				"Q, " +
				"SUM(Anz_KH_FAELLE) AS Anz_KH_FAELLE " + 
			"FROM ( " +
				"SELECT Bezugsjahr, " +
					"CASE WHEN EXTRACT(MONTH FROM Entlassungsdatum) IN (1,2,3) THEN 1 " +
					"WHEN EXTRACT(MONTH FROM Entlassungsdatum) IN (4,5,6) THEN 2 " +
					"WHEN EXTRACT(MONTH FROM Entlassungsdatum) IN (7,8,9) THEN 3 " +
					"WHEN EXTRACT(MONTH FROM Entlassungsdatum) IN (10,11,12) THEN 4 " +
					"END AS Q, " +
					"COUNT(DISTINCT kh_fall_id) AS Anz_KH_FAELLE " +
				"FROM ACC_FDB.AVK_FDB_T_KH_FALL t1 " + 
				"WHERE flag_${flag} = 1 and " +
					"Bezugsjahr IN (${years}) " + 
				"GROUP BY Bezugsjahr, Q ) AS VO " +
			"GROUP BY Bezugsjahr, Q " +
			"ORDER BY Bezugsjahr, Q;";
		
		public static final String FDB_STATISTICS_FOR_ARZT_FALL = "SELECT Bezugsjahr, " +
				"Behandl_Quartal, " +
				"COUNT(DISTINCT EFN_ID) AS Anz_EFN " +
			"FROM ACC_FDB.AVK_FDB_T_ARZT_FALL AF " + 
			"WHERE flag_${flag} = 1 and " +
				"Bezugsjahr IN (${years}) AND " +
				"Vertrags_ID='KV' " +
			"GROUP BY 1,2" +
			"ORDER BY 1,2;";
		
		public static final String FDB_STATISTICS_FOR_AU_FALL = "SELECT Bezugsjahr, "
				+ "Quartal, "
				+ "SUM(ANZ_AU) AS Anz_AU_Faelle "
				+ "FROM ( "
					+ "SELECT Bezugsjahr, "
						+ "CASE WHEN EXTRACT(MONTH FROM LEISTUNGS_BEGINN) IN ('01','02','03') THEN 1 "
						+ "WHEN EXTRACT(MONTH FROM LEISTUNGS_BEGINN) IN ('04','05','06') THEN 2 "
						+ "WHEN EXTRACT(MONTH FROM LEISTUNGS_BEGINN) IN ('07','08','09') THEN 3 "
						+ "WHEN EXTRACT(MONTH FROM LEISTUNGS_BEGINN) IN ('10','11','12') THEN 4 END as Quartal, "
						+ "COUNT(DISTINCT PID||LEISTUNGS_BEGINN) AS Anz_AU "
					+ "FROM ACC_FDB.AVK_FDB_T_AU_Fall_LEIST "
					+ "WHERE flag_${flag} = 1 and "
						+ "Bezugsjahr IN (${years}) "
					+ "GROUP BY Bezugsjahr, Quartal ) AS AU "
				+ "GROUP BY Bezugsjahr, Quartal "
				+ "ORDER BY Bezugsjahr, Quartal;";
		
		public static final String FDB_STATISTICS_FOR_AM_EVO = "SELECT Bezugsjahr, "
				+ "Q, "
				+ "SUM(ANZ_VO) AS Anz_AM_VO "
				+ "FROM ("
					+ "SELECT Bezugsjahr, "
						+ "Verordnungsdatum, "
						+ "CASE WHEN EXTRACT(MONTH FROM Verordnungsdatum) IN (1,2,3) THEN 1 "
						+ "WHEN EXTRACT(MONTH FROM Verordnungsdatum) IN (4,5,6) THEN 2 "
						+ "WHEN EXTRACT(MONTH FROM Verordnungsdatum) IN (7,8,9) THEN 3 "
						+ "WHEN EXTRACT(MONTH FROM Verordnungsdatum) IN (10,11,12) THEN 4 END AS Q, "
						+ "COUNT(DISTINCT PID||PZN||LANR) AS Anz_VO "
					+ "FROM ACC_FDB.AVK_FDB_T_AM_EVO "
					+ "WHERE flag_${flag} = 1 and "
						+ "Bezugsjahr IN (${years}) "
					+ "GROUP BY Bezugsjahr, "
						+ "Verordnungsdatum, Q) AS VO "
				+ "GROUP BY Bezugsjahr, Q "
				+ "ORDER BY Bezugsjahr, Q;";
		
		public static final String FDB_OUTPATIENT_DATA_BY_KV_QUERY = "select coalesce(a.bezugsjahr, 0) as bezugsjahr, "
				+ "coalesce(a.behandl_quartal, 0) as quartal, "
				+ "lukv.kv, "
				+ "lukv.kv_name, "
				+ "coalesce(a.anz_efn, 0) as anz_efn "
				+ "from ("
					+ "select af.bezugsjahr, "
						+ "af.behandl_quartal, "
						+ "af.kv, "
						+ "count(distinct af.efn_id) as anz_efn "
					+ "from acc_fdb.avk_fdb_t_arzt_fall af, "
						+ "(select top 1 a.bezugsjahr, "
						+ "a.behandl_quartal, "
						+ "a.bezugsjahr - 1 as prevYear "
						+ "from acc_fdb.avk_fdb_t_arzt_fall a "
						+ "where a.flag_${flag} = 1 and "
							+ "a.vertrags_id = 'KV' "
						+ "order by a.bezugsjahr desc, "
							+ "a.behandl_quartal desc) latest "
						+ "where af.flag_${flag} = 1 and "
							+ "(af.bezugsjahr = latest.bezugsjahr and af.behandl_quartal = latest.behandl_quartal or "
							+ "af.bezugsjahr = latest.prevYear and af.behandl_quartal = latest.behandl_quartal) and "
							+ "af.vertrags_id='kv' "
						+ "group by 1,2,3 ) a "
				+ "right join acc_adb.av_lu_kv lukv "
				+ "on lukv.kv = a.kv "
				+ "where lukv.km6_id <> '' and "
					+ "(lukv.hinweis is null or lukv.hinweis like '%ab%') "
				+ "order by 3;";
		
		public static final String HINT_CAPTION = "Hinweis";
		public static final String HINT_CONTENT = "Die Tabellen zeigen den aktuellen Datenbestand des jeweiligen Datenbereiches je Quartal. "
				+ "Die relativen Zahlen in den Klammern hinter den Absolutwerten beschreiben dabei das Verh\u00e4ltnis zum Vorjahresquartal. "
				+ "F\u00fcr die nach KV aufgeschl\u00fcsselten Daten befindet sich dieser Wert in der Spalte 'Anteil VJQ'.";
	}
	
	public final class CCI {
		public static final String CCI_PATTERN_TABLE = "etl_icdpatterns";
		
		public static final String CREATE_CCI_ICD_PATTERN_TABLE = "create table %s." + CCI_PATTERN_TABLE + " " +
				"(icd_code varchar(30), "+
				"disease_class varchar(200), " +
				"cci_weight INT);";
		public static final String SELECT_ADB_ARZT_DIAG_WEIGHTS = "select a.pid, a.behandl_quartal, a.bezugsjahr, a.icd_code, b.disease_class, b.cci_weight, a.h2ik "+
				"from acc_adb.avk_adb_t_arzt_diagnose a "+
				"join %s." + CCI_PATTERN_TABLE + " b "+
				"on a.icd_code like b.icd_code "+
				"where a.diagnosesicherheit='G' and "+
				"a.h2ik in %s and "+
				"((a.bezugsjahr = %d and a.behandl_quartal >= %d) or "+
				"(a.bezugsjahr = %d and a.behandl_quartal <= %d));"; 
		public static final String SELECT_ADB_KH_DIAG_WEIGHTS = "select a.pid, "+
				"case "+
				"when extract(month from a.entlassungsdatum) in ('01', '02', '03') then 1 "+
				"when extract(month from a.entlassungsdatum) in ('04', '05', '06') then 2 "+
				"when extract(month from a.entlassungsdatum) in ('07', '08', '09') then 3 "+
				"when extract(month from a.entlassungsdatum) in ('10', '11', '12') then 4 "+
				"end as behandl_quartal, "+
				" a.bezugsjahr, a.icd_code, b.disease_class, b.cci_weight, a.h2ik "+
				"from acc_adb.avk_adb_t_kh_diagnose a "+
				"join %s." + CCI_PATTERN_TABLE + " b "+
				"on a.icd_code like b.icd_code "+
				"where a.h2ik in %s and "+
				"((a.bezugsjahr = %d and behandl_quartal >= %d) or "+
				"(a.bezugsjahr = %d and behandl_quartal <= %d));";
		
		public static final String SELECT_FDB_ARZT_DIAG_WEIGHTS = "select a.pid, a.behandl_quartal, a.bezugsjahr, a.icd_code, b.disease_class, b.cci_weight "+
				"from acc_fdb.avk_fdb_t_arzt_diagnose a "+
				"join %s." + CCI_PATTERN_TABLE + " b "+
				"on a.icd_code like b.icd_code "+
				"where flag_%s=1 and "+
				"((a.bezugsjahr = %d and a.behandl_quartal >= %d) or "+
				"(a.bezugsjahr = %d and a.behandl_quartal <= %d))";
		
		public static final String SELECT_FDB_KH_DIAG_WEIGHTS = "select a.pid, "+
				"case "+
				"when extract(month from a.entlassungsdatum) in ('01', '02', '03') then 1 "+
				"when extract(month from a.entlassungsdatum) in ('04', '05', '06') then 2 "+
				"when extract(month from a.entlassungsdatum) in ('07', '08', '09') then 3 "+
				"when extract(month from a.entlassungsdatum) in ('10', '11', '12') then 4 "+
				"end as behandl_quartal, "+
				"a.bezugsjahr, a.icd_code, b.disease_class, b.cci_weight"+
				"from acc_fdb.avk_fdb_t_kh_diagnose a "+
				"join %s." + CCI_PATTERN_TABLE + " b "+
				"on a.icd_code like b.icd_code "+
				"where flag_%s = 1 and "+
				"((a.bezugsjahr = %d and behandl_quartal >= %d) or "+
				"(a.bezugsjahr = %d and behandl_quartal <= %d));";
		
		public static final String INSERT_CCI_ICD_PATTERNS = "insert into %s." + CCI_PATTERN_TABLE + " values ('%s', '%s', %d);";
		public static final String DELETE_CCI_ICD_PATTERNS = "drop table %s." + CCI_PATTERN_TABLE + ";";

	}
	
	public static final class Hashes {
		public static final String QUERY_PIDS = "select distinct a.pid " +
                "from acc_$db.AVK_$db_T_Vers_Stamm a " +
                "where a.$subgroup and a.bezugsjahr >= $year;";
		public static final String SELECT_MINIMUM_DATES_AND_GENDER = "select "
				+ "a.pid, "
				+ "case "
					+ "when a.minDOB is not null then "
						+ "trim(extract(year from a.minDOB)) || '-' || "
						+ "case "
							+ "when extract(month from a.minDOB) in ('01', '02', '03') then '01' "
							+ "when extract(month from a.minDOB) in ('04', '05', '06') then '02' "
							+ "when extract(month from a.minDOB) in ('07', '08', '09') then '03' "
							+ "when extract(month from a.minDOB) in ('10', '11', '12') then '04' "
						+ "end "
						+ "|| '-' || '01' "
				+ "else '1900-01-01' end as DOB, "
				+ "case "
				+ "when a.minDOD is not null then "
					+ "trim(extract(year from a.minDOD)) || '-' || "
					+ "case "
						+ "when extract(month from a.minDOD) in ('01', '02', '03') then '01' "
						+ "when extract(month from a.minDOD) in ('04', '05', '06') then '02' "
						+ "when extract(month from a.minDOD) in ('07', '08', '09') then '03' "
						+ "when extract(month from a.minDOD) in ('10', '11', '12') then '04' "
					+ "end "
					+ "|| '-' || '01' "
				+ "else '2999-12-31' end as DOD, "
				+ "coalesce(a.minGeschlecht, '0') as minGeschlecht "
				+ "from ("
					+ "select "
					+ "a.pid, min(a.geburtsdatum) as minDOB, "
					+ "min(a.sterbedatum) as minDOD, "
					+ "min(a.geschlecht) as minGeschlecht " 
					+ "from acc_$db.AVK_$db_T_Vers_Stamm a "
					+ "where a.bezugsjahr = $year and a.pid = '$pid' "
					+ "group by a.pid) a";
		
		public static final String SELECT_MIN_MAX_KGS = "select "
				+ "a.pid, "
				+ "case "
					+ " when (min(a.kgs) is not null and min(a.kgs) <> '') then min(a.kgs) "
				+ "else '00000' end as minKgs, "
				+ "case "
					+ " when (max(a.kgs) is not null and max(a.kgs) <> '') then max(a.kgs) "
				+ "else '00000' end as maxKgs "
				+ "from acc_$db.AVK_$db_T_Vers_Region a "
				+ "where a.bezugsjahr = $year and a.pid = '$pid' "
				+ "group by a.pid;";
		
		public static final String SELECT_ICD_CODES = "select distinct "
				+ "a.pid, "
				+ "a.icd_code "
				+ "from acc_$db.AVK_$db_T_Arzt_Diagnose a "
				+ "where a.bezugsjahr = $year "
				+ "and a.diagnosesicherheit='G' "
				+ "and upper(trim(a.vertrags_id))='KV' "
				+ "and a.pid = '$pid' "
				+ "union "
				+ "select b.pid, '0000' as icd_code "
				+ "from (select a.pid, count(distinct a.icd_code) as icdCount "
					+ "from acc_$db.AVK_$db_T_Arzt_Diagnose a "
					+ "where a.bezugsjahr = $year and a.pid = '$pid' "
					+ "group by 1 "
					+ "having icdCount = 0"
				+ ") b "
				+ "order by 1, 2;";
		
		public static final String SELECT_PZNS = "select distinct "
				+ "a.pid, "
				+ "a.pzn "
				+ "from acc_$db.AVK_$db_T_AM_EVO a "
				+ "where a.bezugsjahr = $year and a.pid = '$pid' "
				+ "union "
				+ "select b.pid, '00000000' as icd_code "
				+ "from (select a.pid, count(distinct a.pzn) as pznCount "
					+ "from acc_$db.AVK_$db_T_AM_EVO a "
					+ "where a.bezugsjahr = $year and a.pid = '$pid' "
					+ "group by 1"
					+ "having pznCount = 0"
				+ ") b "
				+ "order by 1, 2;";
	}
	
	public static final class Filter {
		public static final String SELECT_EMPLOYEE_PIDS = "SELECT "
			    + "CASE "
		        + "WHEN LENGTH(CAST(PID AS VARCHAR(10)))=1 THEN TRIM('000000000'||CAST(PID AS CHAR(1))) " 
		        + "WHEN LENGTH(CAST(PID AS VARCHAR(10)))=2 THEN TRIM('00000000'||CAST(PID AS CHAR(2))) "
		        + "WHEN LENGTH(CAST(PID AS VARCHAR(10)))=3 THEN TRIM('0000000'||CAST(PID AS CHAR(3))) "
		        + "WHEN LENGTH(CAST(PID AS VARCHAR(10)))=4 THEN TRIM('000000'||CAST(PID AS CHAR(4))) "
		        + "WHEN LENGTH(CAST(PID AS VARCHAR(10)))=5 THEN TRIM('00000'||CAST(PID AS CHAR(5))) " 
		        + "WHEN LENGTH(CAST(PID AS VARCHAR(10)))=6 THEN TRIM('0000'||CAST(PID AS CHAR(6))) "
		        + "WHEN LENGTH(CAST(PID AS VARCHAR(10)))=7 THEN TRIM('000'||CAST(PID AS CHAR(7))) "
		        + "WHEN LENGTH(CAST(PID AS VARCHAR(10)))=8 THEN TRIM('00'||CAST(PID AS CHAR(8))) "   
		        + "WHEN LENGTH(CAST(PID AS VARCHAR(10)))=9 THEN TRIM('0'||CAST(PID AS CHAR(9))) "    
		        + "WHEN LENGTH(CAST(PID AS VARCHAR(10)))=10 THEN CAST(PID AS CHAR(10)) "
		        + "END AS PID, "
		        + "H2ik "
		        + "FROM ACC_ADB.AVK_ADB_PID_AKTUELL "
		        + "WHERE (EGK_NR_MA<>'' OR KV_NUMMER_MA<>'');";

	}
}
