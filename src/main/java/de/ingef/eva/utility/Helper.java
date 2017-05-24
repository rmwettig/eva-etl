package de.ingef.eva.utility;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public final class Helper {
		
	public static void createFolders(Path path) throws IOException {
		if(Files.notExists(path))
			Files.createDirectories(path);
	}
	
	/**
	 * Calculates years which lie between start and end
	 * @param start
	 * @param end
	 * @return
	 */
	@Deprecated
	public static int[] extractYears(int start, int end) {
		// include start and end year
		int delta = end - start + 1;
		int[] years = new int[delta];
		for (int i = 0; i < delta; i++) {
			years[i] = start + i;
		}
		
		return years;
	}
	
	public static List<Integer> interpolateYears(int start, int end) {
		return IntStream
				.range(start, end + 1)
				.boxed()
				.collect(Collectors.toList());
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
				});
	}
	
	public static Set<String> createUniqueLookupFromFile(String path) {
		Set<String> lookup = new HashSet<String>();
		try(BufferedReader reader = new BufferedReader(new FileReader(path))) {
			String line;
			while((line = reader.readLine()) != null) {
				lookup.add(line.trim());
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return lookup;
	}
		
	public static String joinIks(List<String> iks) {
		return iks.stream().map(ik -> "'" + ik + "'").collect(Collectors.joining(","));
	}
}
