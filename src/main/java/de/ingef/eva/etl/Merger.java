package de.ingef.eva.etl;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import de.ingef.eva.configuration.Configuration;
import de.ingef.eva.constant.OutputDirectory;
import de.ingef.eva.constant.OutputDirectory.DirectoryType;
import de.ingef.eva.utility.Helper;
import de.ingef.eva.utility.IOManager;
import de.ingef.eva.utility.progress.ProgressBar;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;

/**
 * Merges table year slices into one file
 * @author Martin.Wettig
 *
 */
@Log4j2
public class Merger {
	
	private static class DatasetLeafDirectory implements FileVisitor<Path> {
		
		@Getter
		private List<Path> leafDirectories = new ArrayList<>();
		
		@Override
		public FileVisitResult postVisitDirectory(Path arg0, IOException arg1) throws IOException {
			//find subdirectories
			List<Path> subfolders = Files.list(arg0).filter(child -> Files.isDirectory(child)).collect(Collectors.toList());
			//if current folder arg0 has no further directories it is a leaf
			if(subfolders.isEmpty())
				leafDirectories.add(arg0);
			return FileVisitResult.CONTINUE;
		}

		@Override
		public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
			return FileVisitResult.CONTINUE;
		}

		@Override
		public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
			return FileVisitResult.CONTINUE;
		}

		@Override
		public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
			return FileVisitResult.CONTINUE;
		}
	}
	
	@Getter
	@RequiredArgsConstructor
	@Log4j2
	private static class Dataset {
		private final String db;
		private final String datasetName;
		private final String fileName;
		private final List<Path> files;
		
		public long calculateSize() {
			return files.stream()
					.map(this::getFileSize)
					.collect(Collectors.summingLong(size -> size));
		}
		
		private long getFileSize(Path path) {
			try {
				return Files.size(path);
			} catch (IOException e) {
				log.error("Could not determine file size. ", e);
			}
			return 0L;
		}
	}
	
	public void run(Configuration config, IOManager ioManager) {		
		try {
			ExecutorService threadPool = Helper.createThreadPool(config.getThreadCount(), true);
			List<Path> datasetLeaves = readDatasetDirectories(ioManager.getDirectory(DirectoryType.CACHE));
			List<Dataset> datasets = findDatasets(datasetLeaves, createSliceSelectionLookup(config));
			ProgressBar progress = new ProgressBar(datasets.size());
			createMergeTasks(ioManager, datasets, threadPool, progress);
			threadPool.shutdown();
			threadPool.awaitTermination(3, TimeUnit.DAYS);
		} catch(IOException e) {
			log.error("Could not parse datasets. {}", e);
		} catch (InterruptedException e) {
			log.error("Merging took too long. {}", e);
		}
		
	}
	
	/**
	 * creates a set of names of all configured views
	 * @param config
	 * @return
	 */
	private Set<String> createSliceSelectionLookup(Configuration config) {
		return config
				.getExport()
				.getSources()
				.stream()
				.flatMap(source -> source.getViews().stream())
				.map(view -> view.getName())
				.map(this::appendDb) //quick and dirty way to add the db prefix. should not be needed when tables are written using just the relevant name part
				.collect(Collectors.toSet());
	}

	private String appendDb(String viewName) {
		if(viewName.contains("ADB"))
			return "ACC_ADB_" + viewName;
		if(viewName.contains("FDB"))
			return "ACC_FDB_" + viewName;
		return viewName;
	}
	
	private void createMergeTasks(IOManager ioManager, List<Dataset> datasets, ExecutorService threadPool, ProgressBar progressBar) throws IOException {
		for(Dataset ds : datasets) {
			if(ds.calculateSize() == 0) {
				log.warn("Table '{}' in dataset '{}' is empty and will not be merged.", ds.getFileName(), ds.getDatasetName());
				continue;
			}
			Path directory = ioManager.createSubdirectories(DirectoryType.PRODUCTION, ds.getDb(), ds.getDatasetName());
			CompletableFuture.supplyAsync(() -> {
					BufferedWriter writer = null;
					try {
						writer = Files.newBufferedWriter(directory.resolve(ds.getFileName() + ".csv"), OutputDirectory.DATA_CHARSET);
						boolean wasHeaderWritten = false;
						for(Path slice : ds.getFiles()) {
							BufferedReader reader = Files.newBufferedReader(slice);
							//remove header if it was already written
							if(wasHeaderWritten) {
								reader.readLine();
							} else {
								writer.write(reader.readLine());
								writer.write("\n");
								wasHeaderWritten = true;
							}
							String line = null;
							while( (line = reader.readLine()) != null) {
								writer.write(line);
								writer.write("\n");
							}
							reader.close();
						}
					} catch (IOException e) {
						throw new RuntimeException(e);
					} finally {
						try {
							if(writer != null)
								writer.close();
						} catch (IOException e) {
							log.error("Could not close merge output. {}", e);
						}
						progressBar.increase();
					}
					return null;
				},
				threadPool
			).exceptionally(e -> {
				log.error("Could not merge '{}'. ", ds.getFileName(), e);
				return null;
			});
		}
	}

	/**
	 * Finds all terminal dataset directories
	 * @param outputDirectory root directory of output
	 * @return paths with exemplary shape 'out/raw/ADB/Bosch'
	 * @throws IOException
	 */
	private List<Path> readDatasetDirectories(Path outputDirectory) throws IOException {
		DatasetLeafDirectory leafFinder = new DatasetLeafDirectory();
		Files.walkFileTree(outputDirectory, leafFinder);
		return leafFinder.getLeafDirectories();
	}
	
	private List<Dataset> findDatasets(List<Path> rawDatasetDirectories, Set<String> selectedSlicePrefixes) {
		List<Dataset> datasets = new ArrayList<>();
		for(Path p : rawDatasetDirectories) {
			datasets.addAll(findDatasetsInDirectory(p, selectedSlicePrefixes));
		}
		return datasets;
	}
	
	/**
	 * Creates datasets for the given dataset directory
	 * @param datasetDirectory directory that contains the year slices
	 * @param selectedSlicePrefixes slices must partially match the names in the lookup
	 * @return
	 */
	private List<Dataset> findDatasetsInDirectory(Path datasetDirectory, Set<String> selectedSlicePrefixes) {
		Map<String,Dataset> commonName2dataset = new HashMap<>();
		List<File> slices = findSelectedSlices(datasetDirectory, selectedSlicePrefixes);
		for(File slice : slices) {
			String fileName = slice.getName();
			if(!fileName.endsWith(".csv"))
				continue;
			String commonName = extractCommonName(fileName);
			//skip already known datasets
			if(commonName2dataset.containsKey(commonName)) {
				commonName2dataset.get(commonName).getFiles().add(datasetDirectory.resolve(fileName));
			} else {
				List<Path> files = new ArrayList<>();
				files.add(datasetDirectory.resolve(fileName));
				Dataset ds = new Dataset(
						datasetDirectory.getName(1).toString(),
						datasetDirectory.getName(2).toString(),
						commonName,
						files);
				commonName2dataset.put(commonName, ds);
			}
		}
		return new ArrayList<>(commonName2dataset.values());
	}

	private List<File> findSelectedSlices(Path datasetDirectory, Set<String> selectedSlicePrefixes) {
		File[] slices = datasetDirectory.toFile().listFiles();
		List<File> selectedSlices = new ArrayList<>(slices.length);
		for(File slice : slices) {
			String commonName = extractCommonName(slice.getName());
			if(selectedSlicePrefixes.contains(commonName))
				selectedSlices.add(slice);
		}
		return selectedSlices;
	}
	
	/**
	 * extracts the common prefix of file names like 'prefix.yyyy.csv'
	 * @param fileName
	 * @return
	 */
	private String extractCommonName(String fileName) {
		return fileName.substring(0, fileName.indexOf("."));
	}
}
