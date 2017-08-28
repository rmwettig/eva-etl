package de.ingef.eva.configuration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.util.Collection;

import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;

import de.ingef.eva.error.InvalidConfigurationException;

public class ConfigurationTest {

	
	@Test
	public void testIdMapping() throws JsonProcessingException, IOException, InvalidConfigurationException {
		String jsonFile = "src/test/resources/configuration/mapping/mapping.json";
		Configuration config = Configuration.loadFromJson(jsonFile);
		
		Collection<Mapping> mappings = config.getMappings();
		assertNotNull(mappings);
		assertEquals(1, mappings.size());
		
		for(Mapping m : mappings) {
			assertEquals("src/test/resources/configuration/mapping/egk2pid.csv", m.getMappingFileName());
			assertEquals("egk_nr", m.getSourceColumn());
			assertEquals("pid", m.getTargetColumn());
			
			Collection<Target> targets = m.getTargets();
			assertNotNull(targets);
			assertEquals(2, targets.size());
			int i = 0;
			for(Target t : targets) {
				if (i == 0) {
					assertEquals("src/test/resources/configuration/mapping/unmapped1.csv", t.getDataFile());
					assertEquals("src/test/resources/configuration/mapping/unmapped1.header.csv", t.getHeaderFile());
				}
				
				if (i == 1) {
					assertEquals("src/test/resources/configuration/mapping/unmapped2.csv", t.getDataFile());
					assertEquals("src/test/resources/configuration/mapping/unmapped2.header.csv", t.getHeaderFile());
				}
				i++;
			}
		}
	}
}
