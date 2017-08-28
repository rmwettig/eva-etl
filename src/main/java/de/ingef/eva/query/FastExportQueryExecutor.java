package de.ingef.eva.query;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import de.ingef.eva.async.AsyncFastExportJob;
import de.ingef.eva.configuration.Configuration;
import de.ingef.eva.constant.OutputDirectory;
import de.ingef.eva.data.DataTable;
import de.ingef.eva.error.QueryExecutionException;
import de.ingef.eva.utility.Helper;

/**
 * Starts FastExport jobs for each submitted job. The upper limit is specified by thread count in configuration.
 * Since each FastExport is run in separated process the total number of threads used amounts to 2*thread count.
 * 
 * @author Martin.Wettig
 *
 */
public class FastExportQueryExecutor implements QueryExecutor<QueryJob> {
	
	private Configuration configuration;
	private ExecutorService threadPool;
	private CountDownLatch cdl;
	
	public FastExportQueryExecutor(Configuration config, ExecutorService threadPool, int jobCount) {
		configuration = config;
		cdl = new CountDownLatch(jobCount);
		this.threadPool = threadPool;
	}
	
	@Override
	public DataTable execute(QueryJob query) throws QueryExecutionException {
		String fileName = query.getPath().substring(query.getPath().lastIndexOf("\\") + 1, query.getPath().lastIndexOf("."));
		String logDirectory = configuration.getLogDirectory() + "/" + OutputDirectory.FEXP_LOGS;
		Helper.createFolders(logDirectory);
		String errorFile = logDirectory + "/" + fileName + ".err";
		threadPool.execute(new AsyncFastExportJob(errorFile, query.getPath(), cdl));
		return null;
	}
	
	public void shutdown() throws QueryExecutionException {
		try {
			cdl.await(3, TimeUnit.DAYS);
		} catch (InterruptedException e) {
			throw new QueryExecutionException("FastExport jobs take too long.", e);
		}
	}
}
