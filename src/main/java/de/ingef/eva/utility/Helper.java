package de.ingef.eva.utility;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.stream.Collectors;

public final class Helper {
	
	public static void createFolders(String path) {
		File f = new File(path);
		if(!f.exists())
			f.mkdirs();
	}
					
	/**
	 * Calculates years which lie between start and end
	 * @param start
	 * @param end
	 * @return
	 */
	public static int[] extractYears(int start, int end) {
		// include start and end year
		int delta = end - start + 1;
		int[] years = new int[delta];
		for (int i = 0; i < delta; i++) {
			years[i] = start + i;
		}
		
		return years;
	}
	
	public static boolean areCredentialsCorrect(String user, String password, String url) {
		Connection conn = null;
		try {
			Class.forName("com.teradata.jdbc.TeraDriver");
			conn = DriverManager.getConnection(
					url,
					user,
					password
			);
			conn.close();
			return true;
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			try {
				if(conn != null && !conn.isClosed()) conn.close();
			} catch (SQLException e1) {
				e1.printStackTrace();
				return false;
			}
			return false;
		}
		return false;
	}
	
	public static ExecutorService createThreadPool(int size, boolean spawnDaemonThreads) {
		return Executors.newFixedThreadPool(
				size,
				new ThreadFactory() {
					
					@Override
					public Thread newThread(Runnable r) {
						Thread t = new Thread(r);
						t.setDaemon(spawnDaemonThreads);
						return t;
					}
				}
			);
	}
	
	public static String joinIks(List<String> iks) {
		return iks.stream().map(ik -> "'" + ik + "'").collect(Collectors.joining(","));
	}
}
