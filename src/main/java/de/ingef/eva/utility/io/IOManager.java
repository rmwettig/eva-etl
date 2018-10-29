package de.ingef.eva.utility.io;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import de.ingef.eva.configuration.Configuration;
import de.ingef.eva.constant.OutputDirectory.DirectoryType;
import lombok.extern.log4j.Log4j2;

/**
 * manages application directories
 */
@Log4j2
public class IOManager {

	private Map<DirectoryType,Path> directories = new HashMap<>(3);
	
	public static IOManager of(Configuration config) {
		IOManager manager = new IOManager();
		manager.directories.put(DirectoryType.CACHE, ensureValidPath(DirectoryType.CACHE, config.getCacheDirectory()));
		manager.directories.put(DirectoryType.PRODUCTION, ensureValidPath(DirectoryType.PRODUCTION, config.getOutputDirectory()));
		manager.directories.put(DirectoryType.REPORT, ensureValidPath(DirectoryType.REPORT, config.getReportDirectory()));
		return manager;
	}
	
	public Path getDirectory(DirectoryType type) {
		Path path = directories.get(type);
		createIfNotExists(path);
		return path;
	}
	
	public Path createSubdirectories(DirectoryType type, String... subdirectories) {
		Path extendedRoot = Paths.get(directories.get(type).toString(), subdirectories);
		createIfNotExists(extendedRoot);
		return extendedRoot;
	}
	
	private void createIfNotExists(Path path) {
		try {
			if(Files.notExists(path))
				Files.createDirectories(path);
		} catch (IOException e) {
			log.error("Could not create directory '{}'. ", path.toString(), e);
		}
	}
	
	/**
	 * creates a valid path, i.e. if given path is null or empty a new directory for the directory type is inserted
	 * @param type defines for what the directory is used
	 * @param userDefinedPath path specified by user
	 * @return user path or current working directory with subfolder named according to given type
	 */
	private static Path ensureValidPath(DirectoryType type, String userDefinedPath) {
		if(userDefinedPath != null && !userDefinedPath.isEmpty())
			return Paths.get(userDefinedPath);
		return Paths.get(System.getProperty("user.dir"), type.name());
	}
}
