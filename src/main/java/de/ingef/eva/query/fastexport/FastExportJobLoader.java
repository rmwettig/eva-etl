package de.ingef.eva.query.fastexport;

import java.io.File;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.Collection;

import de.ingef.eva.configuration.Configuration;
import de.ingef.eva.constant.OutputDirectory;
import de.ingef.eva.query.QueryJob;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class FastExportJobLoader {
	
	public Collection<QueryJob> loadFastExportJobs(Configuration configuration) {
		File directory = new File(configuration.getOutputDirectory() + "/" + OutputDirectory.FEXP_JOBS);
		if(!directory.exists()) {
			log.error("Could not load FastExport scripts. Path '{}' does not exist.", directory.getPath());
		}
		File[] fastExportJobs = directory.listFiles(new FilenameFilter() {
			
			@Override
			public boolean accept(File dir, String name) {
				return name.endsWith(".fx");
			}
		});
		
		Collection<QueryJob> jobs = new ArrayList<>(fastExportJobs.length);
		for(File f : fastExportJobs)
			jobs.add(new QueryJob(f.getAbsolutePath()));
		
		log.info("{} FastExport jobs loaded.", jobs.size());
		return jobs;
	}
}
