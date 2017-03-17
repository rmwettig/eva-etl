package de.ingef.eva.utility;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * A container that associates different data files along with its header
 * 
 * @author Martin Wettig
 *
 */
public class Dataset {
	private File _headerFile;
	private List<File> _data;
	private String _name;

	public Dataset(String name) {
		_data = new ArrayList<File>();
		_name = name;
	}

	public File getHeaderFile() {
		return _headerFile;
	}

	public void setHeaderFile(File f) {
		_headerFile = f;
	}

	public List<File> getData() {
		return _data;
	}

	public boolean addFile(File f) {
		return _data.add(f);
	}

	public String getName() {
		return _name;
	}
}
