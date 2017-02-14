package de.ingef.eva.writer;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;

public class CsvWriter implements ResultWriter {

	public void Write(Collection<String[]> rows, String filename) {
		try {
			BufferedWriter writer = new BufferedWriter(new FileWriter(filename));
			for (String[] row : rows)
			{
				StringBuilder sb = new StringBuilder();
				for (int i = 0; i < row.length; i++)
				{
					sb.append(row[i]);
					if (i != row.length - 1)
						sb.append(';');
				}
				sb.append('\n');
				writer.write(sb.toString());
			}
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
