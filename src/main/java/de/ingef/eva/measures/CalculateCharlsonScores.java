package de.ingef.eva.measures;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import de.ingef.eva.configuration.Configuration;
import lombok.extern.log4j.Log4j2;

@Log4j2
public final class CalculateCharlsonScores {
	private static final String QUERY_TEMPLATE = "select a.pid, a.behandl_quartal, a.bezugsjahr, a.icd_code, b.disease_class, b.cci_weight, b.icd_code from acc_adb.avk_adb_t_arzt_diagnose a, 	(select pid, disease_class, cci_weight, icd_code from sb_hri.mw_adb_arztdiag_weights_tmp) b where a.h2ik = (select distinct h2ik from acc_adb.av_lu_kasse where kassenname='%s') and a.diagnosesicherheit='G' and a.pid=b.pid and a.icd_code=b.icd_code order by a.bezugsjahr, a.behandl_quartal;";
	private static final String[] INSURANCES = new String[]{ "Bosch BKK", "BKK Salzgitter" };
		
	
	public static void calculate(Configuration config, ExecutorService threadPool) {
		try {
			Class.forName("com.teradata.jdbc.TeraDriver");
			try (
				Connection conn = DriverManager.getConnection(config.getFullConnectionUrl(), config.getUser(), config.getPassword());
				Statement query = conn.createStatement();
			) {
				for(String hi : INSURANCES) {
					ResultSet result = query.executeQuery(String.format(QUERY_TEMPLATE, hi));
					Collection<String> scores = calculateMorbiscores(result);
					BufferedWriter writer = new BufferedWriter(new FileWriter(config.getOutputDirectory()+String.format("/charlsonscores.%s.csv", hi)));
					writer.write("pid;Q_Start;Q_End;Score\n");
					for(String line : scores) {
						writer.write(line);
						writer.newLine();
					}
					
					writer.close();
				}
				
			} catch (SQLException e) {
				log.error("Could not open connection or creating query.\n\tReason: {}", e.getMessage());
			} catch (IOException e1) {
				log.error("Could not write charlsonscores.csv.\n\tReason: {}", e1.getMessage());
			}
			
		} catch (ClassNotFoundException e) {
			log.error("Did not found Teradata JDBC driver.");
		}
	}
	
	private static Collection<String> calculateMorbiscores(ResultSet result) throws SQLException {
		int columnCount = result.getMetaData().getColumnCount();
		Map<String,CharlsonScore> pid2score = new HashMap<String,CharlsonScore>();
		
		while(result.next()) {
			//columns are: pid, quarter, year, icd, disease class, weight
			for(int i = 1; i <= columnCount; i++) {
				result.getString(i);
			}
			String pid = result.getString(1);
			int quarter = result.getInt(2);
			int year = result.getInt(3);
			String diseaseClass = result.getString(5);
			int weight = result.getInt(6);
			
			if(pid2score.containsKey(pid))
				pid2score.get(pid).updateWeightOrAddEntry(quarter, year, diseaseClass, weight);
			else {
				CharlsonScore ms = new CharlsonScore();
				ms.updateWeightOrAddEntry(quarter, year, diseaseClass, weight);
				pid2score.put(pid, ms);
			}
		}
		
		return scoresToRows(pid2score);
	}
	
	private static Collection<String> scoresToRows(Map<String,CharlsonScore> pid2score) {
		List<String> rows = new ArrayList<String>();
		for(String pid : pid2score.keySet()) {
			CharlsonScore ms = pid2score.get(pid);
			for(String score : ms.calculateSlidingScore())
				rows.add(pid +";"+score);
		}
		
		return rows;
	}
}
