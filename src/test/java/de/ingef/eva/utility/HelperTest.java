package de.ingef.eva.utility;

import static org.junit.Assert.*;

import java.io.File;

import org.junit.Test;

import de.ingef.eva.utility.Helper;

public class HelperTest {

	@Test
	public void testCreateFolders() {
		String path = "src/test/resources/foldercreation/";
		Helper.createFolders(path);
		File f = new File(path);
		assertTrue(f.exists());
		f.delete();
	}

}
