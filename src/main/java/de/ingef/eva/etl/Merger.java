package de.ingef.eva.etl;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import de.ingef.eva.configuration.Configuration;
import de.ingef.eva.constant.OutputDirectory;
import de.ingef.eva.utility.Helper;
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
	
	public void run(Configuration config) {		
		try {
			ExecutorService threadPool = Helper.createThreadPool(config.getThreadCount(), true);
			List<Path> datasetLeaves = readDatasetDirectories(config.getOutputDirectory());
			List<Dataset> datasets = findDatasets(datasetLeaves);
			createMergeTasks(config.getOutputDirectory(), datasets, threadPool);
			threadPool.shutdown();
			threadPool.awaitTermination(3, TimeUnit.DAYS);
		} catch(IOException e) {
			log.error("Could not parse datasets. {}", e);
		} catch (InterruptedException e) {
			log.error("Merging took too long. {}", e);
		}
		
	}
	
	private void createMergeTasks(String rootDirectory, List<Dataset> datasets, ExecutorService threadPool) throws IOException {
		for(Dataset ds : datasets) {
			if(ds.calculateSize() == 0) {
				log.warn("Table '{}' in dataset '{}' is empty and will not be merged.", ds.getFileName(), ds.getDatasetName());
				continue;
			}
			Path directory = createMergeDirectories(rootDirectory, ds);
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

	private Path createMergeDirectories(String rootDirectory, Dataset ds) throws IOException {
		Path output = Paths.get(rootDirectory, OutputDirectory.PRODUCTION, ds.getDb(), ds.getDatasetName());
		if(!Files.exists(output))
			Files.createDirectories(output);
		return output;
	}

	/**
	 * Finds all terminal dataset directories
	 * @param outputDirectory root directory of output
	 * @return paths with exemplary shape 'out/raw/ADB/Bosch'
	 * @throws IOException
	 */
	private List<Path> readDatasetDirectories(String outputDirectory) throws IOException {
		Path root = Paths.get(outputDirectory, OutputDirectory.RAW);
		DatasetLeafDirectory leafFinder = new DatasetLeafDirectory();
		Files.walkFileTree(root, leafFinder);
		return leafFinder.getLeafDirectories();
	}
	
	private List<Dataset> findDatasets(List<Path> rawDatasetDirectories) {
		List<Dataset> datasets = new ArrayList<>();
		for(Path p : rawDatasetDirectories) {
			datasets.addAll(findDatasetsInDirectory(p));
		}
		return datasets;
	}
	
	/**
	 * Creates datasets for the given dataset directory
	 * @param datasetDirectory directory that contains the year slices
	 * @return
	 */
	private List<Dataset> findDatasetsInDirectory(Path datasetDirectory) {
		Map<String,Dataset> commonName2dataset = new HashMap<>();
		File[] slices = datasetDirectory.toFile().listFiles();
		for(File slice : slices) {
			String fileName = slice.getName();
			if(!fileName.endsWith(".csv"))
				continue;
			String commonName = fileName.substring(0, fileName.indexOf("."));
			//skip already known datasets
			if(commonName2dataset.containsKey(commonName)) {
				commonName2dataset.get(commonName).getFiles().add(datasetDirectory.resolve(fileName));
			} else {
				List<Path> files = new ArrayList<>();
				files.add(datasetDirectory.resolve(fileName));
				Dataset ds = new Dataset(
						datasetDirectory.getName(2).toString(),
						datasetDirectory.getName(3).toString(),
						commonName,
						files);
				commonName2dataset.put(commonName, ds);
			}
		}
		return new ArrayList<>(commonName2dataset.values());
	}
	
}
