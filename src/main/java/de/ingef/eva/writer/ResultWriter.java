package de.ingef.eva.writer;

import java.util.Collection;

public interface ResultWriter {
	void Write(Collection<String[]> rows, String filename);
}
