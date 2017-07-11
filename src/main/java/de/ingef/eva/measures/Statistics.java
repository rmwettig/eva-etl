package de.ingef.eva.measures;

import de.ingef.eva.configuration.Configuration;
import de.ingef.eva.data.DataTable;
import de.ingef.eva.datasource.DataSource;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class Statistics implements DataSource {
	private static final String[] TABLES = new String[]{"himi_evo", "hemi_evo", "am_evo", "kh_fall", "arzt_fall", "au_fall"};
	private final Configuration configuration;
	private final String table;
	private final String column;
	
	@Override
	public DataTable fetchData() {
		// TODO Auto-generated method stub
		return null;
	}

}
